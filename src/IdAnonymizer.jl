module IdAnonymizer

using DataFrames
using CSV
using Random
using SHA

export anonymize_ids, deanonymize_ids, compress_ids, restore_ids

function get_id_digits(n::Int64)::Int64
  return length(string(n))
end

function anonymize_ids(df::DataFrame)::Tuple{DataFrame,Dict{Int64,String}}
  shuffled_df = df[shuffle(1:nrow(df)), :]
  n = nrow(shuffled_df)
  digits = get_id_digits(n)

  id_map = Dict{Int64,String}()
  used_anonymous_ids = Set{String}()

  for id in shuffled_df.IDs
    while true
      hash = bytes2hex(sha256(string(id)))
      anonymous_id = "S" * hash[1:10]  # Use more characters for better uniqueness
      if !(anonymous_id in used_anonymous_ids)
        push!(used_anonymous_ids, anonymous_id)
        id_map[id] = anonymous_id
        break
      end
    end
  end

  anonymized_df = DataFrame(
    IDs=[id_map[id] for id in shuffled_df.IDs],
    Ans=shuffled_df.Ans
  )
  return anonymized_df, id_map
end

function deanonymize_ids(
  df::DataFrame, id_map::Dict{Int64,String}
)::DataFrame
  reverse_map = Dict(v => k for (k, v) in id_map)

  deanonymized_df = copy(df)
  deanonymized_df.IDs = [reverse_map[id] for id in df.IDs]

  return deanonymized_df
end

"""
    compress_ids(df::DataFrame; id_col::Symbol=:IDs, prefix::String="S")

Maps potentially long IDs in the DataFrame to short sequential IDs (e.g., S1, S2)
to save context window space. Returns the modified DataFrame and a dictionary
mapping the short IDs back to the original IDs.
"""
function compress_ids(df::DataFrame; id_col::Symbol=:IDs, prefix::String="S")::Tuple{DataFrame, Dict{String, String}}
    if !hasproperty(df, id_col)
        error("Column $id_col not found in DataFrame")
    end

    unique_ids = unique(df[!, id_col])
    # Mapping: Short ID -> Original ID (for restoration)
    short_to_long = Dict{String, String}()
    # Mapping: Original ID -> Short ID (for compression)
    long_to_short = Dict{Any, String}()

    for (i, original_id) in enumerate(unique_ids)
        short_id = string(prefix, i)
        str_original_id = string(original_id)
        short_to_long[short_id] = str_original_id
        long_to_short[original_id] = short_id
    end

    compressed_df = copy(df)
    compressed_df[!, id_col] = [long_to_short[id] for id in df[!, id_col]]

    return compressed_df, short_to_long
end

"""
    restore_ids(df::DataFrame, map::Dict{String, String}; id_col::Symbol=:IDs)

Restores original IDs from short IDs using the provided mapping dictionary.
"""
function restore_ids(df::DataFrame, map::Dict{String, String}; id_col::Symbol=:IDs)::DataFrame
    if !hasproperty(df, id_col)
        error("Column $id_col not found in DataFrame")
    end

    restored_df = copy(df)
    # Use get with default to handle cases where ID might not be in map (though it should be)
    restored_df[!, id_col] = [get(map, string(id), string(id)) for id in df[!, id_col]]

    return restored_df
end

end
