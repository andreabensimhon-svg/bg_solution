// Dataflow Gen2 : Parse JSON Orders → lh_sales.silver
let
    Source = Lakehouse.Contents(null){[workspaceId = #"workspace_id"]}[Data]{[lakehouseId = #"lh_sales_id"]}[Data],
    raw_orders = Source{[Name = "raw_json_orders"]}[Data],
    ParsedJson = Table.AddColumn(raw_orders, "parsed", each try Json.Document([order_payload]) otherwise null),
    ValidRows = Table.SelectRows(ParsedJson, each [parsed] <> null),
    Expanded = Table.ExpandRecordColumn(ValidRows, "parsed", {"order_id", "client_id", "order_date", "delivery_date", "status", "total_amount", "currency"}),
    Typed = Table.TransformColumnTypes(Expanded, {{"order_id", type text}, {"client_id", type text}, {"order_date", type date}, {"total_amount", type number}}),
    WithMetadata = Table.AddColumn(Typed, "_cleaned_ts", each DateTime.LocalNow(), type datetime)
in
    WithMetadata
