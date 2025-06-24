from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "health_metrics" and
      r._field == "steptime"
  )
  |> set(key: "_measurement", value: "step_time")
  |> to(bucket: "health_data", org: "MyOrg")

