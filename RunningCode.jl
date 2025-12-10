using DataFrames
using YAML
using PromptingTools
using CSV
using AES
using JSON

import ClaudePrompting.IdCipher as IC
import ClaudePrompting.PrepareData as PD
import ClaudePrompting.CommunicateClaude as CC
import ClaudePrompting.IdAnonymizer as IA

# 1. Prepare your data

# rawdata
csv_filepath = "/home/kjeong/localgit/ClaudePrompting.jl/week2_res.csv"
# Claude 3 API
api_filepath = "/home/kjeong/.config/anthropic/anthropic_api_key"
# Prompt for Claude action
prompt_filepath = "/home/kjeong/localgit/ClaudePrompting.jl/prompt_W2"

df = CSV.read(csv_filepath, DataFrame)

# 2. Data loading and encryption or anonymization
# 2.1. Encryption by AES
ids_vec = string.(df[!, :IDs])
key = IC.generate_key()
encrypted_df = PD.encrypt_input_data(df, key)

open("aes_key.bin", "w")do io  # Save the AES key to a file
  write(io, key.key)
end

loaded_key = open("aes_key.bin", "r") do io  # Load the AES key
  key_bytes = read(io, 16)
  AES128Key(key_bytes)
end

# 2.2. Anonymization
anonymized_df, id_map = IA.anonymize_ids(df)

open("id_map.json", "w") do io  # Save the id_map to json file
  JSON.print(io, id_map)
end

id_map_dict = open("id_map.json", "r") do io # Load the saved id_map
  JSON.parse(io, dicttype=Dict{String, String})
end
Dict(parse(Int64, k) => v for (k, v) in id_map_dict)


deanonymized_df = IA.deanonymize_ids(anonymized_df, id_map)
df


# 3. Convert the DataFrame into YAML format
# Two ID processing methods are available, so you
# need to choose the appropriate dataframe name for the
# following process. For example, AES encryption would
# use `encrypted_df` and anonymization would use
# `anonymized_df`.

key_order = [:IDs, :Ans]
yml_data = PD.df_to_ordered_dicts(anonymized_df, key_order)
yml_string = YAML.yaml(yml_data)

# 4. Setup Claude connection

CC.set_anthropic_api_key(api_filepath)
batch_size = 10   # Set the number of data points to send to Claude at once
prompt = read(prompt_filepath, String)
responses = CC.process_sequential_batches(yml_string, prompt, batch_size)

open("W2_responses1.txt", "w") do io   # Save responses into a file
  for (i, response) in enumerate(responses)
    write(io, response)
    if i < length(responses)
      write(io, "\n")  # insert a new line in-between responses
    end
  end
end

# Clean YAML data before merge using the below code
# (Remove the ```yaml\n and \n``` parts)

# 5. Merge the responses into a single YAML file

responses_load = YAML.load_file("W2_responses.txt")



all_keys = unique(vcat([collect(keys(response)) for response in responses_load]...))

# 6. Create a DataFrame from the merged YAML data

desired_order = ["IDs", "Achievement", "Suggestion", "Writing_ability", "Remarks"]
column_order = [key for key in desired_order if key in all_keys]
df = DataFrame([key => Union{String, Missing}[] for key in column_order])

for response in responses_load   # add each dict to the DataFrame
  row = Dict(key => get(response, key, missing) for key in column_order)
  push!(df, row)
end


deanonymized_df = IA.deanonymize_ids(df, id_map)
open("commentary_w2.csv", "w") do io   # Save the comments into a CSV file
  CSV.write(io, deanonymized_df)
end

# 7. Decrypt the IDs using the AES key

encrypted_ids = df[!, :IDs] 
decrypted_ids = [IC.decrypt_id(id, key) for id in encrypted_ids]
df.IDs = decrypted_ids

open("commentary.csv", "w") do io   # Save the comments into a CSV file
  CSV.write(io, df)
end
