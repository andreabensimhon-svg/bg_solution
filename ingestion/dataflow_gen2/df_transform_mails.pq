// ============================================================
// BGSolutions - Dataflow Gen2 : Transformation légère Mails
// Power Query M pour parser les mails bruts
// ============================================================

let
    // Source : table Bronze raw_mails
    Source = Lakehouse.Contents(null){[workspaceId = #"workspace_id"]}[Data]{[lakehouseId = #"lakehouse_id"]}[Data],
    raw_mails = Source{[Name = "raw_mails"]}[Data],

    // Typage des colonnes
    TypedColumns = Table.TransformColumnTypes(raw_mails, {
        {"mail_id", Int64.Type},
        {"date_envoi", type datetime}
    }),

    // Nettoyage du corps du mail
    CleanedBody = Table.AddColumn(TypedColumns, "corps_nettoye", each
        Text.Clean(
            Text.Replace(
                Text.Replace([corps], "#(cr)#(lf)", " "),
                "#(tab)", " "
            )
        ),
        type text
    ),

    // Comptage des pièces jointes
    PJCount = Table.AddColumn(CleanedBody, "nb_pieces_jointes", each
        try List.Count(Json.Document([pieces_jointes])) otherwise 0,
        Int64.Type
    ),

    // Détection de langue simplifiée
    LangueDetection = Table.AddColumn(PJCount, "langue_detectee", each
        if Text.Contains([corps_nettoye], " le ") or Text.Contains([corps_nettoye], " de ") then "fr"
        else if Text.Contains([corps_nettoye], " the ") or Text.Contains([corps_nettoye], " of ") then "en"
        else "unknown",
        type text
    ),

    // Sélection des colonnes finales
    FinalColumns = Table.SelectColumns(LangueDetection, {
        "mail_id", "expediteur", "destinataire", "sujet",
        "corps_nettoye", "date_envoi",
        "nb_pieces_jointes", "langue_detectee"
    }),

    // Ajout métadonnées
    WithMetadata = Table.AddColumn(FinalColumns, "_cleaned_ts", each DateTime.LocalNow(), type datetime)

in
    WithMetadata
