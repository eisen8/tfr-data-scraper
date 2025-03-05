import json
import os
import time
import traceback

from dotenv import load_dotenv

from google import genai

from common.print_helper import tprint, tprintln
from common.constants import Constants as C
from common.database import Database as DB
from src.tfr_data_scraper.common.time_helper import format_time, get_timestamp

if __name__ == "__main__":
    # Dev Notes: Consider using context cashing for the input prompt
    # Dev Notes: Consider configuring the output to a Json schema explicitly

    # Config
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    temperature = 0.2
    top_p = 0.8
    max_requests = 21
    max_fails = 3
    sleep_time_seconds = 5  # current free tier limit: 15 Requests per minute 	1,000,000 Tokens per minute 	1,500 requests per day
    data_input_chunk_size = 30  # num of data inputs per request

    start_time = time.time()
    os.makedirs(C.RESPONSES_FOLDER_PATH, exist_ok=True)
    with open(C.DATA_FOLDER_PATH / 's7_prompt.txt', 'r') as file:
        base_prompt = file.read()

    if api_key is None:
        raise Exception("Missing Gemini API Key. Make sure to create a .env with GEMINI_API_KEY set to your API key")

    client = genai.Client(api_key=api_key)

    #  Chunk data into multiple requests
    fails = 0
    total_tokens = 0
    index = 0
    file_names_processed = 0
    DB.open_db()
    total_filenames_to_annotate = DB.get_count_of_files_to_annotate()
    tprint(f"{total_filenames_to_annotate} file to annotate")
    while True:
        try:
            filenames = DB.get_files_to_annotate(data_input_chunk_size)
            if not filenames:
                break

            file_names_processed += len(filenames)
            prompt = base_prompt + "\n" + '\n'.join(filenames)
            tprint(f"Sending request to Gemini for chunk {index}")
            ts = time.perf_counter()
            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt,
                config={
                    "temperature": temperature,
                    "top_p": top_p
                }
            )
            tprint(f"API call took {(time.perf_counter() - ts):.1f} seconds.")
            if response.usage_metadata:
                tprint(f"Tokens used: {response.usage_metadata.total_token_count} (prompt: {response.usage_metadata.prompt_token_count}, output: {response.usage_metadata.candidates_token_count})")
                total_tokens += response.usage_metadata.total_token_count
            else:
                tprint("Usage metadata not available.")

            cleaned_str = response.text.strip("```json\n").strip("```")

            # Temp for verification
            with open(C.RESPONSES_FOLDER_PATH / f"response_{get_timestamp()}.txt", "w") as f:
                f.write(cleaned_str)

            try:
                json_list = json.loads(cleaned_str)
                annot_dict = {}
                for filename in filenames:
                    annotation = next((obj for obj in json_list if obj.get("filename") == filename), None)
                    if annotation:
                        DB.add_annotation(filename, json.dumps(annotation, indent=4))
                    else:
                        tprint(f"Could not find annotation for filename {filename}")

            except Exception as e:
                fails += 1
                tprint(f"Error with response Json for {index} - {e}\n{traceback.format_exc()}")

        except Exception as e:
            fails += 1
            tprint(f"Exception for chunk {index} - {e}\n{traceback.format_exc()}")

        if fails >= max_fails:
            tprint(f"Failed {fails} times. Stopping.")
            break
        else:
            time_to_sleep = (sleep_time_seconds - (time.time() - start_time))
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
            index += 1

        if index >= max_requests:
            tprint(f"Completed max requests of {index}")
            break

    tprintln()
    tprint(f"---- Script has finished. ----")
    tprint(f"Run time: {format_time(time.time() - start_time)}")
    tprint(f"Results: ")
    tprint(f"{file_names_processed} FileNames Processed.")
    tprint(f"{index} Responses handled")
    tprint(f"{total_tokens} Tokens used")

