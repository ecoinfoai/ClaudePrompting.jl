module PrepareData

using CSV
using DataFrames
using AES
using OrderedCollections

import ClaudePrompting.IdCipher as IC
import ClaudePrompting.IdAnonymizer as IA
export encrypt_input_data, df_to_yml, check_incomplete_responses,
       check_duplicate_anonymous_ids, compress_input_data

function encrypt_input_data(df::DataFrame, ids_vec::Vector{String}, key::AES.AES128Key)
  encrypted_ids = IC.encrypt_ids(ids_vec, key)
  encrypted_df = copy(df)
  encrypted_df[!, :IDs] = encrypted_ids

  return encrypted_df
end

"""
    compress_input_data(df::DataFrame; id_col::Symbol=:IDs)

Compresses the input DataFrame by mapping potentially long encrypted IDs to short
sequential IDs using IdAnonymizer.compress_ids. Returns the compressed DataFrame
and the mapping for later restoration.
"""
function compress_input_data(df::DataFrame; id_col::Symbol=:IDs)
  return IA.compress_ids(df, id_col=id_col)
end


function df_to_yml(df::DataFrame, key_order::Vector{Symbol})::Vector{OrderedDict{Symbol, Any}}
  yml_data = Vector{OrderedDict{Symbol, Any}}(undef, nrow(df))
  for (i, row) in enumerate(eachrow(df))
    ordered_dict = OrderedDict{Symbol, Any}(key => row[key] for key in key_order)
    yml_data[i] = ordered_dict
  end
  return yml_data
end

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
