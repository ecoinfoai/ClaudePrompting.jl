module PrepareData

using CSV
using DataFrames
using AES
using OrderedCollections

import ClaudePrompting.IdCipher as IC
export encrypt_input_data, df_to_yml, check_incomplete_responses,
       check_duplicate_anonymous_ids
export compress_input_data

"""
    encrypt_input_data(df::DataFrame, ids_vec::Vector{String}, key::AES.AES128Key)

Encrypt the input data IDs using AES encryption.
"""
function encrypt_input_data(df::DataFrame, ids_vec::Vector{String}, key::AES.AES128Key)
  encrypted_ids = [IC.encrypt_id(id, key) for id in ids_vec]
  encrypted_df = copy(df)
  encrypted_df[!, :IDs] = encrypted_ids

  return encrypted_df
end

"""
    compress_input_data(df::DataFrame, ids_vec::Vector{String}, key::AES.AES128Key)

Alias for `encrypt_input_data`.
"""
const compress_input_data = encrypt_input_data

"""
    df_to_yml(df::DataFrame, key_order::Vector{Symbol})::Vector{Any}

Convert a DataFrame to a YAML-compatible structure (Vector of OrderedDictionaries).
"""
function df_to_yml(df::DataFrame, key_order::Vector{Symbol})::Vector{Any}
  yml_data = []
  for row in eachrow(df)
    ordered_dict = OrderedDict(key => row[key] for key in key_order)
    push!(yml_data, ordered_dict)
  end
  return yml_data
end

"""
    check_incomplete_responses(data::Vector{Dict{Any,Any}})::Dict{String,Vector{String}}

Check for incomplete responses in the data. Returns a dictionary where keys are IDs and values are lists of missing fields.
"""
function check_incomplete_responses(
  data::Vector{Dict{Any,Any}}
)::Dict{String,Vector{String}}

  incomplete_responses = Dict{String,Vector{String}}()

  for item in data
    required_keys = [
      "IDs", "Achievement", "Feedback_rationale", "Suggestion", "SorW", "Remarks"
    ]

    incomplete_features = String[]

    for key in required_keys
      if !haskey(item, key) || (isa(item[key], String) && strip(item[key]) == "")
        push!(incomplete_features, key)
      end
    end

    if !isempty(incomplete_features)
      id = get(item, "IDs", "Unknown")
      incomplete_responses[id] = incomplete_features
    end

  end

  return incomplete_responses
end

"""
    check_duplicate_anonymous_ids(id_map::Dict{Int64,String})

Check for duplicate anonymous IDs in the ID map. Prints the result to stdout.
"""
function check_duplicate_anonymous_ids(id_map::Dict{Int64,String})
  anonymous_ids = values(id_map)
  unique_ids = Set(anonymous_ids)

  if length(anonymous_ids) == length(unique_ids)
    println("All anonymized IDs are unique. No duplicates found.")
  else
    println("Duplicate anonymized IDs have been detected.")

    # Finding duplicate IDs
    id_counts = Dict{String,Int}()
    for id in anonymous_ids
      id_counts[id] = get(id_counts, id, 0) + 1
    end

    duplicates = filter(pair -> pair.second > 1, id_counts)

    println("Duplicate IDs and their counts:")
    for (id, count) in duplicates
      println("$id: $count times")
    end
  end
end

end
