from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "health_metrics" and
      r._field == "uprtime"
  )
  |> set(key: "_measurement", value: "upr_time")
  |> to(bucket: "health_data", org: "MyOrg")

