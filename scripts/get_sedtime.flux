from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "health_metrics" and
      r._field == "sedtime"
  )
  |> set(key: "_measurement", value: "sed_time")
  |> to(bucket: "health_data", org: "MyOrg")

