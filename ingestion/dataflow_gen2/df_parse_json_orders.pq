// ============================================================
// BGSolutions - Dataflow Gen2 : Ingestion JSON Orders
// Power Query M pour parser les commandes JSON
// ============================================================

let
    // Source : table Bronze raw_json_orders
    Source = Lakehouse.Contents(null){[workspaceId = #"workspace_id"]}[Data]{[lakehouseId = #"lakehouse_id"]}[Data],
    raw_orders = Source{[Name = "raw_json_orders"]}[Data],

    // Parse du JSON payload
    ParsedJson = Table.AddColumn(raw_orders, "parsed", each
        try Json.Document([order_payload]) otherwise null
    ),

    // Filtrer les lignes avec JSON invalide
    ValidRows = Table.SelectRows(ParsedJson, each [parsed] <> null),

    // Expansion des champs JSON
    Expanded = Table.ExpandRecordColumn(ValidRows, "parsed", {
        "order_id", "client_id", "order_date", "delivery_date",
        "status", "total_amount", "currency", "items"
    }),

    // Typage
    TypedColumns = Table.TransformColumnTypes(Expanded, {
        {"order_id", type text},
        {"client_id", type text},
        {"order_date", type date},
        {"delivery_date", type date},
        {"total_amount", type number}
    }),

    // Sélection colonnes finales
    FinalColumns = Table.SelectColumns(TypedColumns, {
        "order_id", "client_id", "order_date", "delivery_date",
        "status", "total_amount", "currency", "source_system", "received_at"
    }),

    WithMetadata = Table.AddColumn(FinalColumns, "_cleaned_ts", each DateTime.LocalNow(), type datetime)

in
    WithMetadata
