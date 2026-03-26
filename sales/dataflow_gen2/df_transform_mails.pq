// Dataflow Gen2 : Parse Mails → lh_sales.silver.clean_mails_parsed
let
    Source = Lakehouse.Contents(null){[workspaceId = #"workspace_id"]}[Data]{[lakehouseId = #"lh_sales_id"]}[Data],
    raw_mails = Source{[Name = "raw_mails"]}[Data],
    Typed = Table.TransformColumnTypes(raw_mails, {{"mail_id", Int64.Type}, {"date_envoi", type datetime}}),
    CleanedBody = Table.AddColumn(Typed, "corps_nettoye", each Text.Clean(Text.Replace(Text.Replace([corps], "#(cr)#(lf)", " "), "#(tab)", " ")), type text),
    PJCount = Table.AddColumn(CleanedBody, "nb_pieces_jointes", each try List.Count(Json.Document([pieces_jointes])) otherwise 0, Int64.Type),
    Langue = Table.AddColumn(PJCount, "langue_detectee", each
        if Text.Contains([corps_nettoye], " le ") or Text.Contains([corps_nettoye], " de ") then "fr"
        else if Text.Contains([corps_nettoye], " the ") then "en" else "unknown", type text),
    Final = Table.SelectColumns(Langue, {"mail_id", "expediteur", "destinataire", "sujet", "corps_nettoye", "date_envoi", "nb_pieces_jointes", "langue_detectee"}),
    WithMeta = Table.AddColumn(Final, "_cleaned_ts", each DateTime.LocalNow(), type datetime)
in
    WithMeta
