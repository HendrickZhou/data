from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "calm"
  )
  |> set(key: "_measurement", value: "calm")
  |> to(bucket: "health_data", org: "MyOrg")


from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "tired"
  )
  |> set(key: "_measurement", value: "tired")
  |> to(bucket: "health_data", org: "MyOrg")


from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "lonely"
  )
  |> set(key: "_measurement", value: "lonely")
  |> to(bucket: "health_data", org: "MyOrg")


from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "pain"
  )
  |> set(key: "_measurement", value: "pain")
  |> to(bucket: "health_data", org: "MyOrg")

from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "control"
  )
  |> set(key: "_measurement", value: "control")
  |> to(bucket: "health_data", org: "MyOrg")


from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "feel"
  )
  |> set(key: "_measurement", value: "feel")
  |> to(bucket: "health_data", org: "MyOrg")

from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "where_now"
  )
  |> set(key: "_measurement", value: "where_now")
  |> to(bucket: "health_data", org: "MyOrg")

from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "whowith_now"
  )
  |> set(key: "_measurement", value: "whowith_now")
  |> to(bucket: "health_data", org: "MyOrg")


"""
from(bucket: "health_data")
  |> range(start: 0)  // Adjust as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      (r._field == "thoughtsemotions" or r._field == "physical" or r._field == "present")
  )
  |> pivot(
      rowKey:["_time", "userID"],
      columnKey: ["_field"],
      valueColumn: "_value"
  )
  |> map(fn: (r) => ({
      _time: r._time,
      _value: r.thoughtsemotions + r.physical + r.present,
      _measurement: "mindfulness",
      _field: "score", 
      userID: r.userID
  }))
  |> to(bucket: "health_data", org: "MyOrg")
  """
  # actually we already has this _field
  from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "mindfulness"
  )
  |> set(key: "_measurement", value: "mindfulness")
  |> to(bucket: "health_data", org: "MyOrg")


  from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "per_cog"
  )
  |> set(key: "_measurement", value: "per_cog")
  |> to(bucket: "health_data", org: "MyOrg")

  
  from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "high_arousal_pos"
  )
  |> set(key: "_measurement", value: "high_arousal_pos")
  |> to(bucket: "health_data", org: "MyOrg")

  
  from(bucket: "health_data")
  |> range(start: 0)  // Adjust time range as needed
  |> filter(fn: (r) =>
      r._measurement == "ema_data" and
      r._field == "negative_affect"
  )
  |> set(key: "_measurement", value: "negative_affect")
  |> to(bucket: "health_data", org: "MyOrg")