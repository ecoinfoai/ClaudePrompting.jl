module PrepareData

using CSV
using DataFrames
using AES
using OrderedCollections

import ClaudePrompting.IdCipher as IC

export encrypt_input_data, df_to_ordered_dicts, check_incomplete_responses, check_duplicate_anonymous_ids

"""
    encrypt_input_data(df::DataFrame, key::AES.AES128Key; id_column::Symbol=:IDs)::DataFrame

Encrypts a specified column in a DataFrame using AES-128.
"""
function encrypt_input_data(df::DataFrame, key::AES.AES128Key; id_column::Symbol=:IDs)::DataFrame
  encrypted_ids = [IC.encrypt_id(string(id), key) for id in df[!, id_column]]
  encrypted_df = copy(df)
  encrypted_df[!, id_column] = encrypted_ids
  return encrypted_df
end

"""
    df_to_ordered_dicts(df::DataFrame, key_order::Vector{Symbol})::Vector{OrderedDict{Symbol, Any}}

Converts a DataFrame to a vector of ordered dictionaries.
"""
function df_to_ordered_dicts(df::DataFrame, key_order::Vector{Symbol})::Vector{OrderedDict{Symbol, Any}}
  ordered_dicts = []
  for row in eachrow(df)
    ordered_dict = OrderedDict(key => row[key] for key in key_order)
    push!(ordered_dicts, ordered_dict)
  end
  return ordered_dicts
end

"""
    check_incomplete_responses(
        data::Vector{<:Dict},
        required_keys::Vector{String};
        id_key::String="IDs"
    )::Dict{String,Vector{String}}

Checks for incomplete responses in a vector of dictionaries.
"""
function check_incomplete_responses(
    data::Vector{<:Dict},
    required_keys::Vector{String};
    id_key::String="IDs"
)::Dict{String,Vector{String}}
    incomplete_responses = Dict{String,Vector{String}}()

    for item in data
        incomplete_features = String[]
        for key in required_keys
            if !haskey(item, key) || (isa(item[key], String) && isempty(strip(item[key])))
                push!(incomplete_features, key)
            end
        end

        if !isempty(incomplete_features)
            id = get(item, id_key, "Unknown")
            incomplete_responses[id] = incomplete_features
        end
    end

    return incomplete_responses
end

"""
    check_duplicate_anonymous_ids(id_map::Dict)::Dict

Checks for duplicate anonymous IDs in a dictionary.
"""
function check_duplicate_anonymous_ids(id_map::Dict)::Dict
    anonymous_ids = collect(values(id_map))
    id_counts = Dict{String,Int}()
    for id in anonymous_ids
        id_counts[id] = get(id_counts, id, 0) + 1
    end

    duplicates = filter(p -> p.second > 1, id_counts)

    return duplicates
end

end