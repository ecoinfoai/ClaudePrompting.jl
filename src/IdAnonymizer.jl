module IdAnonymizer

using DataFrames
using CSV
using Random
using SHA

export anonymize_ids, deanonymize_ids
export compress_ids, restore_ids

function get_id_digits(n::Int64)::Int64
  return length(string(n))
end

"""
    anonymize_ids(df::DataFrame)

Anonymize the IDs in the dataframe. Returns a tuple containing the anonymized dataframe and a dictionary mapping original IDs to anonymous IDs.
"""
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

"""
    compress_ids(df::DataFrame)

Alias for `anonymize_ids`.
"""
const compress_ids = anonymize_ids

"""
    deanonymize_ids(df::DataFrame, id_map::Dict{Int64,String})

Deanonymize the IDs in the dataframe using the provided ID map.
"""
function deanonymize_ids(
  df::DataFrame, id_map::Dict{Int64,String}
)::DataFrame
  reverse_map = Dict(v => k for (k, v) in id_map)

  deanonymized_df = copy(df)
  deanonymized_df.IDs = [reverse_map[id] for id in df.IDs]

  return deanonymized_df
end

"""
    restore_ids(df::DataFrame, id_map::Dict{Int64,String})

Alias for `deanonymize_ids`.
"""
const restore_ids = deanonymize_ids

end
