module IdAnonymizer

using DataFrames
using Random
using SHA

export anonymize_ids, deanonymize_ids
export compress_ids, restore_ids

"""
    anonymize_ids(
        df::DataFrame;
        id_column::Symbol=:IDs,
        columns_to_anonymize::Vector{Symbol}=[:Ans]
    )::Tuple{DataFrame,Dict{Any,String}}

Anonymizes a specified ID column in a DataFrame and returns the anonymized DataFrame
along with a map from original to anonymous IDs.
"""
function anonymize_ids(
    df::DataFrame;
    id_column::Symbol=:IDs,
    columns_to_anonymize::Vector{Symbol}=[:Ans]
)::Tuple{DataFrame,Dict{Any,String}}
    shuffled_df = df[shuffle(1:nrow(df)), :]

    id_map = Dict{Any,String}()
    used_anonymous_ids = Set{String}()

    for id in shuffled_df[!, id_column]
        while true
            hash = bytes2hex(sha256(string(id)))
            anonymous_id = "S" * hash[1:10]
            if !(anonymous_id in used_anonymous_ids)
                push!(used_anonymous_ids, anonymous_id)
                id_map[id] = anonymous_id
                break
            end
        end
    end

    anonymized_df = DataFrame()
    anonymized_df[!, id_column] = [id_map[id] for id in shuffled_df[!, id_column]]

    for col in columns_to_anonymize
        anonymized_df[!, col] = shuffled_df[!, col]
    end

    return anonymized_df, id_map
end

"""
    deanonymize_ids(
        df::DataFrame,
        id_map::Dict{Any,String};
        id_column::Symbol=:IDs
    )::DataFrame

Restores original IDs in a DataFrame using a provided ID map.
"""
function deanonymize_ids(
    df::DataFrame,
    id_map::Dict{Any,String};
    id_column::Symbol=:IDs
)::DataFrame
    reverse_map = Dict(v => k for (k, v) in id_map)

    deanonymized_df = copy(df)
    deanonymized_df[!, id_column] = [reverse_map[id] for id in df[!, id_column]]

    return deanonymized_df
end

end
