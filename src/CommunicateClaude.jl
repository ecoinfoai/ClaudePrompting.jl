module CommunicateClaude

using PromptingTools
using YAML
using DataFrames
using JSON3
using HTTP

export set_anthropic_api_key, provide_yml_to_claude, yml_res_to_dataframe, process_sequential_batches

"""
    set_anthropic_api_key(filepath::String)

Sets the Anthropic API key from a file.
"""
function set_anthropic_api_key(filepath::String)
    if isfile(filepath)
        api_key = strip(read(filepath, String))
        PromptingTools.set_preferences!("ANTHROPIC_API_KEY" => api_key)
        println("Anthropic API Key is successfully set.")
    else
        error("API key file not found.")
    end
end

"""
    provide_yml_to_claude(
        yml_string::String,
        prompt::String;
        model::String="claude-3-5-sonnet-20240620",
        retries::Int=3
    )::String

Sends a YAML string to the Claude API and returns the response.
"""
function provide_yml_to_claude(
    yml_string::String,
    prompt::String;
    model::String="claude-3-5-sonnet-20240620",
    retries::Int=3
)::String
    api_key = PromptingTools.get_preferences("ANTHROPIC_API_KEY")
    headers = [
        "Content-Type" => "application/json",
        "x-api-key" => api_key,
        "anthropic-version" => "2023-06-01"
    ]
    body = JSON3.write(Dict(
        "model" => model,
        "max_tokens" => 8000,
        "messages" => [
            Dict("role" => "user", "content" => "$prompt\n\nHere is some data in string originally from yaml format:\n\n$yml_string")
        ]
    ))

    for attempt in 1:retries
        try
            response = HTTP.post("https://api.anthropic.com/v1/messages", headers, body)
            response_body = JSON3.read(String(response.body))

            if haskey(response_body, :content) && length(response_body.content) > 0
                return response_body.content[1].text
            else
                error("Unexpected response structure from Claude API")
            end
        catch e
            if attempt == retries
                rethrow(e)
            end
            println("Attempt $attempt failed. Retrying in 2 seconds...")
            sleep(2)
        end
    end
end

"""
    yml_res_to_dataframe(yml_result::String, col_names::Vector{String})::DataFrame

Converts a YAML string result to a DataFrame.
"""
function yml_res_to_dataframe(yml_result::String, col_names::Vector{String})::DataFrame
    data = YAML.load(yml_result)
    df = DataFrame(
        [Dict(col => get(entry, col, missing) for col in col_names)
         for entry in data]
    )
    df = select!(df, col_names)
    return df
end

"""
    process_sequential_batches(
        yml_data::Vector,
        prompt::String;
        batch_size::Int=10,
        model::String="claude-3-5-sonnet-20240620"
    )::Vector{String}

Processes data in sequential batches and sends to the Claude API.
"""
function process_sequential_batches(
    yml_data::Vector,
    prompt::String;
    batch_size::Int=10,
    model::String="claude-3-5-sonnet-20240620"
)::Vector{String}
    total_items = length(yml_data)
    num_batches = ceil(Int, total_items / batch_size)
    results = String[]

    for i in 1:num_batches
        start_idx = (i - 1) * batch_size + 1
        end_idx = min(i * batch_size, total_items)
        batch = yml_data[start_idx:end_idx]
        batch_yml = YAML.write(batch)

        try
            result = provide_yml_to_claude(batch_yml, prompt, model=model)
            push!(results, result)
            println("Processed batch $i of $num_batches")
        catch e
            println("Error processing batch $i: $e")
        end

        sleep(1) # Rate limiting
    end

    return results
end

end