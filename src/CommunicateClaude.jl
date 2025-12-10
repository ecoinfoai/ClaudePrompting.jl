module CommunicateClaude

using PromptingTools
using YAML
using DataFrames
using JSON3
using HTTP

export set_anthropic_api_key, provide_yml_to_claude, yml_res_to_dataframe,
       process_sequential_batches

function set_anthropic_api_key(filepath::String)
  if isfile(filepath)
    api_key = strip(read(filepath, String))
    PromptingTools.set_preferences!("ANTHROPIC_API_KEY" => api_key)
    println("Anthropic API Key is successfully set.")
  else
    error("API key file not found.")
  end
end

function provide_yml_to_claude(yml_string::String, prompt::String)
  api_key = PromptingTools.get_preferences("ANTHROPIC_API_KEY")
  headers = [
    "Content-Type" => "application/json",
    "x-api-key" => api_key,
    "anthropic-version" => "2023-06-01"
  ]
  body = JSON3.write(Dict(
    #"model" => "claude-3-haiku-20240307", 
    "model" => "claude-3-5-sonnet-20240620",
    "max_tokens" => 8000,
    "messages" => [
      Dict("role" => "user", "content" => "$prompt\n\nHere is some data in string originally from yaml format:\n\n$yml_string")
    ]
  ))
  response = HTTP.post("https://api.anthropic.com/v1/messages", headers, body)
  response_body = JSON3.read(String(response.body))

  if haskey(response_body, :content) && length(response_body.content) > 0
    return response_body.content[1].text
  else
    error("Unexpected response structure from Claude API")
  end

end

function yml_res_to_dataframe(
  yml_result::String, col_names::Vector{String}
)::DataFrame
  data = YAML.load(yml_result)
  df = DataFrame(
    [Dict(col => get(entry, col, missing) for col in col_names)
     for entry in data]
  )
  df = select!(df, col_names)

  return df
end

function process_sequential_batches(
  yml_string::String, prompt::String, batch_size::Int64=10
)
  entries = split(yml_string, "\n- ")
  # split might leave an empty string at start if it starts with "\n- " or "- "
  # The original code did: entries[1] = entries[1][2:end] assuming it starts with "- " but split removes "\n- "
  # Let's look at how df_to_yml produces output. YAML usually produces "- key: value..."

  # Wait, df_to_yml returns Vector{OrderedDict}, not String.
  # process_sequential_batches receives yml_string.
  # Where is the conversion to String happening?
  # The user likely converts the Vector from df_to_yml to a YAML string before passing here.
  # The original code assumed `yml_string` is a big string.

  # Original code:
  # entries = split(yml_string, "\n- ")
  # entries[1] = entries[1][2:end]

  # If yml_string is "- A\n- B\n- C", split by "\n- " gives: ["- A", "B", "C"].
  # entries[1] comes out as "- A". entries[1][2:end] becomes " A".
  # That seems risky if formatting varies.

  # However, preserving original logic logic while optimizing memory.

  if startswith(yml_string, "- ")
      entries = split(yml_string[3:end], "\n- ")
  else
      entries = split(yml_string, "\n- ")
  end
  # This split creates a Vector{SubString} or Vector{String}.

  total_items = length(entries)
  num_batches = ceil(Int, total_items / batch_size)
  results = String[]

  for i in 1:num_batches
    start_idx = (i - 1) * batch_size + 1
    end_idx = min(i * batch_size, total_items)
    batch = entries[start_idx:end_idx]

    # Use IOBuffer to build batch_yml
    io = IOBuffer()
    write(io, "- ")
    join(io, batch, "\n- ")
    batch_yml = String(take!(io))

    try
      result = provide_yml_to_claude(batch_yml, prompt)
      push!(results, result)

      println("Processed batch $i of $num_batches")
    catch e
      println("Error processing batch $i: $e")
    end

    sleep(1)
  end

  return results
end

end
