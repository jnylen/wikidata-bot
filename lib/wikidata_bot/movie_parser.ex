defmodule WikidataBot.MovieParser do
  alias GraphqlBuilder.Query

  @language_mappings %{
    "af" => "af-ZA",
    "ar" => "ar-AE",
    "arz" => "ar-EG",
    "az" => "az-Latn-AZ",
    "azb" => "az-Cyrl-AZ",
    "be" => "be-BY",
    "bg" => "bg-BG",
    "bs" => "bs-Latn-BA",
    "ca" => "ca-ES",
    "cs" => "cs-CZ",
    "da" => "da-DK",
    "de" => "de-DE",
    "dv" => "dv-MV",
    "el" => "el-GR",
    "en" => "en-US",
    "en-ca" => "en-CA",
    "en-gb" => "en-GB",
    "es" => "es-ES",
    "et" => "et-EE",
    "eu" => "eu-ES",
    "fa" => "fa-IR",
    "fi" => "fi-FI",
    "fo" => "fo-FO",
    "fr" => "fr-FR",
    "gl" => "gl-ES",
    "gu" => "gu-IN",
    "he" => "he-IL",
    "hi" => "hi-IN",
    "hr" => "hr-HR",
    "hu" => "hu-HU",
    "hy" => "hy-AM",
    "id" => "id-ID",
    "is" => "is-IS",
    "it" => "it-IT",
    "ja" => "ja-JP",
    "ka" => "ka-GE",
    "kk" => "kk-KZ",
    "kn" => "kn-IN",
    "ko" => "ko-KR",
    "ky" => "ky-KG",
    "lt" => "lt-LT",
    "lv" => "lv-LV",
    "mi" => "mi-NZ",
    "mk" => "mk-MK",
    "mn" => "mn-MN",
    "mr" => "mr-IN",
    "ms" => "ms-MY",
    "mt" => "mt-MT",
    "nn" => "nn-NO",
    "no" => "nb-NO",
    "nl" => "nl-NL",
    "nso" => "ns-ZA",
    "pa" => "pa-IN",
    "pl" => "pl-PL",
    "pt" => "pt-PT",
    "ro" => "ro-RO",
    "ru" => "ru-RU",
    "sa" => "sa-IN",
    "se" => "se-SE",
    "sk" => "sk-SK",
    "sl" => "sl-SI",
    "sv" => "sv-SE",
    # "se" => "se-KE",
    "ta" => "ta-IN",
    "te" => "te-IN",
    "th" => "th-IN",
    "tn" => "tn-ZA",
    "tr" => "tr-TR",
    "tt" => "tt-RU",
    "uk" => "uk-UA",
    "ur" => "ur-PK",
    "vi" => "vi-VN",
    "xh" => "xh-ZA",
    "zh" => "zh-CN",
    "zh-yue" => "zh-HK",
    "zh-min-nan" => "zh-TW",
    "zu" => "zu-ZA"
  }

  def import_movies do
    parse_file()
    # |> Enum.map(&create_movie/1)
  end

  def create_movie(data) do
    label = [
      value: data.name,
      language: "en-US"
    ]

    %Query{
      operation: :create_film,
      variables:
        Map.drop(data, [:name, :production_date])
        |> Map.put(:label, label)
        |> Enum.into([]),
      fields: [:uid]
    }
    |> MetagraphSDK.mutate()
  end

  defp parse_file do
    Application.get_env(:wikidata_bot, :file_path, "results2.json")
    |> File.stream!([:trim_bom])
    |> Stream.map(&parse_json/1)
    |> Stream.map(&IO.inspect/1)
    |> Stream.map(&parse_movie/1)
    |> Stream.map(&IO.inspect/1)
    # |> Stream.map(&IO.inspect/1)
    |> Stream.run()

    []
  end

  defp parse_json("\n"), do: nil
  defp parse_json(string), do: Jason.decode!(string)

  defp parse_movie(map) do
    adult_movie = !is_nil(get_prop("P5083", map))

    result = %{
      "label" => parse_labels(Map.get(map, "labels", [])),
      "wikidata_id" => Map.get(map, "id"),
      "omdb_id" => get_prop("P3302", map) |> to_integer(),
      "imdb_id" => get_prop("P345", map),
      "themoviedb_id" => get_prop("P4947", map) |> to_integer(),
      "freebase_id" => get_prop("P646", map),
      "website" => get_prop("P856", map),
      "adult" => adult_movie
    }

    {_, res} =
      result
      |> Map.keys()
      |> Enum.map_reduce(%{}, fn key, acc ->
        if is_nil(Map.get(result, key)) do
          {nil, acc}
        else
          {nil, Map.put(acc, key, Map.get(result, key))}
        end
      end)

    res
  end

  defp parse_labels(list) do
    list
    |> Map.values()
    |> Enum.map(fn label ->
      %{
        "language" => Map.get(@language_mappings, Map.get(label, "language", "unknown")),
        "value" => Map.get(label, "value")
      }
    end)
    |> Enum.reject(fn label ->
      is_nil(Map.get(label, "language"))
    end)
  end

  defp get_value(%{"mainsnak" => %{"datavalue" => %{"value" => %{"id" => value}}}}), do: value
  defp get_value(%{"mainsnak" => %{"datavalue" => %{"value" => value}}}), do: value

  defp get_prop(prop, %{"claims" => claims}),
    do:
      claims
      |> Map.get(prop, [])
      |> Enum.map(&get_value/1)
      |> List.first()

  defp to_integer(integer) when is_integer(integer), do: integer
  defp to_integer(string) when is_bitstring(string), do: string |> String.to_integer()
  defp to_integer(_), do: nil
end
