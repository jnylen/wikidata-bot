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
    Application.get_env(:wikidata_bot, :file_path, "results.json")
    |> File.stream!([:trim_bom])
    |> Stream.map(&parse_json/1)
    |> Stream.reject(&is_nil/1)
    |> Stream.map(&parse_movie/1)
    |> Stream.map(&create_movie/1)
    |> Stream.run()
  end

  def create_movie({data, relations}) do
    Map.get(data, "wikidata_id")
    |> get_item()
    |> case do
      nil ->
        WikidataBot.Client.create_item(
          "film",
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

    process_relations(Map.get(data, "wikidata_id") |> get_item(), relations)
  end

  def process_relations(nil, _relations), do: nil

  def process_relations(uid, relations) do
    # Director
    (Map.get(relations, "director") || [])
    |> Enum.map(fn director ->
      act_uid = WikidataBot.PersonParser.get_item(director)

      # Exists? Create crew
      if act_uid do
        WikidataBot.Client.create_item(
          "crew",
          %{
            "film" => uid,
            "person" => act_uid,
            "job" => "director"
          }
        )
      end
    end)

    # writer
    (Map.get(relations, "writer") || [])
    |> Enum.map(fn writer ->
      act_uid = WikidataBot.PersonParser.get_item(writer)

      # Exists? Create crew
      if act_uid do
        WikidataBot.Client.create_item(
          "crew",
          %{
            "film" => uid,
            "person" => act_uid,
            "job" => "writer"
          }
        )
      end
    end)

    # producer
    (Map.get(relations, "producer") || [])
    |> Enum.map(fn producer ->
      act_uid = WikidataBot.PersonParser.get_item(producer)

      # Exists? Create crew
      if act_uid do
        WikidataBot.Client.create_item(
          "crew",
          %{
            "film" => uid,
            "person" => act_uid,
            "job" => "producer"
          }
        )
      end
    end)

    # exec_producer
    (Map.get(relations, "exec_producer") || [])
    |> Enum.map(fn exec_producer ->
      act_uid = WikidataBot.PersonParser.get_item(exec_producer)

      # Exists? Create crew
      if act_uid do
        WikidataBot.Client.create_item(
          "crew",
          %{
            "film" => uid,
            "person" => act_uid,
            "job" => "executive_producer"
          }
        )
      end
    end)

    # cast
    (Map.get(relations, "cast") || [])
    |> Enum.map(fn cast ->
      act_uid = WikidataBot.PersonParser.get_item(cast)

      # Exists? Create cast
      if act_uid do
        WikidataBot.Client.create_item(
          "performance",
          %{
            "film" => uid,
            "person" => act_uid
          }
        )
      end
    end)
  end

  defp parse_json("\n"), do: nil
  defp parse_json(string), do: Jason.decode!(string)

  defp parse_movie(nil), do: nil

  defp parse_movie(map) do
    props =
      [
        # director
        "P57",
        # screenwriter
        "P58",
        # cast
        "P161",
        # prod.
        "P162",
        # exec. prod.
        "P1431"
      ]
      |> Enum.reduce([], fn prop, acc ->
        acc
        |> Enum.concat(get_props(prop, map))
      end)
      |> Enum.uniq()

    # Result
    adult_movie = !is_nil(get_prop("P5083", map))

    result = %{
      "label" => parse_labels(Map.get(map, "labels", [])),
      "description" => parse_labels(Map.get(map, "descriptions", [])),
      "wikidata_id" => Map.get(map, "id"),
      "omdb_id" => get_prop("P3302", map) |> to_integer(),
      "imdb_id" => get_prop("P345", map),
      "sfdb_id" => get_prop("P2334", map),
      "themoviedb_id" => get_prop("P4947", map) |> to_integer(),
      "freebase_id" => get_prop("P646", map),
      "elonet_id" => get_prop("P2346", map) |> to_integer(),
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

    # Relations
    relations = %{
      "director" => get_props("P57", map),
      "writer" => get_props("P58", map),
      "cast" => get_props("P161", map),
      "producer" => get_props("P162", map),
      "exec_producer" => get_props("P1431", map)
    }

    # Return
    {
      res,
      relations
    }
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
    "media.wikidata_id"
    |> WikidataBot.Client.query_item(wid)
  end
end
