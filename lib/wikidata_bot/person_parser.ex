defmodule WikidataBot.PersonParser do
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

  def import_persons do
    Application.get_env(:wikidata_bot, :file_path, "results_ids.json")
    |> File.stream!([:trim_bom])
    |> Stream.map(&parse_json/1)
    |> Stream.reject(&is_nil/1)
    |> Stream.map(&parse_person/1)
    |> Stream.map(&create_person/1)
    |> Stream.run()
  end

  def create_person(data) do
    Map.get(data, "wikidata_id")
    |> get_item()
    |> case do
      nil ->
        WikidataBot.Client.create_item(
          "person",
          data
        )

        IO.puts("Inserted #{Map.get(data, "wikidata_id")}")

      uid ->
        WikidataBot.Client.update_item(
          uid,
          data
        )

        IO.puts("Updated #{Map.get(data, "wikidata_id")}")
    end
  end

  defp parse_json("\n"), do: nil
  defp parse_json(string), do: Jason.decode!(string)

  defp parse_person(nil), do: nil

  defp parse_person(map) do
    # Result
    result = %{
      "label" => parse_labels(Map.get(map, "labels", [])),
      "description" => parse_labels(Map.get(map, "descriptions", [])),
      "wikidata_id" => Map.get(map, "id")
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

    # Return
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
    |> Enum.filter(fn label ->
      Enum.member?(
        WikidataBot.Languages.all_codes(),
        Map.get(label, "language")
      )
    end)
  end

  defp get_value(%{"mainsnak" => %{"datavalue" => %{"value" => %{"id" => value}}}}), do: value
  defp get_value(%{"mainsnak" => %{"datavalue" => %{"value" => value}}}), do: value
  defp get_value(_), do: nil

  defp get_prop(prop, %{"claims" => claims}),
    do:
      claims
      |> Map.get(prop, [])
      |> Enum.map(&get_value/1)
      |> List.first()

  defp get_props(prop, %{"claims" => claims}),
    do:
      claims
      |> Map.get(prop, [])
      |> Enum.map(&get_value/1)

  defp to_integer(integer) when is_integer(integer), do: integer

  defp to_integer(string) when is_bitstring(string) do
    String.to_integer(string)
  rescue
    _ -> nil
  end

  defp to_integer(_), do: nil

  def get_item(wid) do
    base = MetagraphSDK.new()

    %GraphqlBuilder.Query{
      operation: :people,
      fields: [:uid],
      variables: [query: wid, query_field: "wikidata_id"]
    }
    |> GraphqlBuilder.query()
    |> Neuron.query(
      %{},
      url: base.url,
      headers: [authorization: "Bearer #{base.token}"]
    )
    |> case do
      {:ok, %{body: %{"data" => %{"people" => result}}}} ->
        (List.first(result) || %{})
        |> Map.get("uid")

      _ ->
        nil
    end
  end
end
