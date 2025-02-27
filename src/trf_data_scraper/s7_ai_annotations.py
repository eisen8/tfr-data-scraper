import json
import os
import time
import traceback

from dotenv import load_dotenv

from google import genai

from common.print_helper import tprint
from common.constants import Constants as C


class annotation:
    text: str
    label: str


class filename_annotated:
    filename: str
    annotations: list[annotation]


if __name__ == "__main__":
    # Dev Notes: Consider using context cashing for the input prompt
    # Dev Notes: Consider configuring the output to a Json schema explicitly

    # Config
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    output_file = C.DATA_FOLDER_PATH / "file_name_annotations.json"
    max_fails = 3
    sleep_time_seconds = 5  # current free tier limit: 15 Requests per minute 	1,000,000 Tokens per minute 	1,500 requests per day
    data_input_chunk_size = 10  # num of data inputs per request

    with open('s7_prompt.txt', 'r') as file:
        base_prompt = file.read()
    with open('s7_data.txt', 'r') as file:
        full_data = file.read()
        filenames = full_data.splitlines()

    if api_key is None:
        raise Exception("Missing Gemini API Key. Make sure to create a .env with GEMINI_API_KEY set to your API key")

    client = genai.Client(api_key=api_key)

    #  Chunk data into multiple requests then combine outputs
    result_prompts = []
    fails = 0
    for i in range(0, len(filenames), data_input_chunk_size):
        try:
            data_chunk = filenames[i: i + data_input_chunk_size]
            prompt = base_prompt + "\n" + '\n'.join(data_chunk)

            tprint(f"Sending request to Gemini for chunk {i}-{i + data_input_chunk_size} out of {len(full_data)}")
            ts = time.perf_counter()
            response = client.models.generate_content(
                model="gemini-2.0-flash",  # Specify the model you want to use
                contents=prompt,
            )
            tprint(f"API call took {(time.perf_counter() - ts):.1f} seconds.")

            result_prompts.append(response.text)
            # tprint(response.text)
            if response.usage_metadata:
                tprint(f"Total tokens used: {response.usage_metadata.total_token_count}")
                tprint(f"Prompt tokens used: {response.usage_metadata.prompt_token_count}")
                tprint(f"Candidates tokens used: {response.usage_metadata.candidates_token_count}")
            else:
                tprint("Usage metadata not available.")

            #with open(C.DATA_FOLDER_PATH / f"response_{i}.txt", "w") as f:
            #    f.write(response.text)

        except Exception as e:
            fails += 1
            tprint(f"Exception for chunk {i}-{i + data_input_chunk_size} out of {len(full_data)} - {e}\n{traceback.format_exc()}")

        if fails >= max_fails:
            tprint(f"Failed {fails} times. Stopping.")
            break
        else:
            time.sleep(sleep_time_seconds)

    # combine results into one and save to file
    combined_list = []
    for json_str in result_prompts:
        cleaned_str = json_str.strip("```json\n").strip("```")  # AI response surrounds the json with this metadata, so remove it
        combined_list.extend(json.loads(cleaned_str))

    # Write the combined list to a file
    with open(output_file, 'w') as f:
        json.dump(combined_list, f, indent=4)
