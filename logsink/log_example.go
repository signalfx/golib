package logsink

// OTELJsonFormat is useful for testing. All the examples are taken from
// https://github.com/tigrannajaryan/rfcs/blob/feature/tigran/log-data-model/text/0097-log-data-model.md#example-log-records
const OTELJsonFormat = ` 
[
  {
    "Timestamp": 1586960586000,
    "Attributes": {
      "http.status_code": 500,
      "http.url": "http://example.com",
      "my.custom.application.tag": "hello"
    },
    "Resource": {
      "service.name": "donut_shop",
      "service.version": "semver:2.0.0",
      "k8s.pod.uid": "1138528c-c36e-11e9-a1a7-42010a800198"
    },
    "TraceId": "f4dbb3edd765f620",
    "SpanId": "43222c2d51a7abe3",
    "SeverityText": "INFO",
    "SeverityNumber": 9,
    "Body": "20200415T072306-0700 INFO I like donuts"
  },
  {
    "Timestamp": 1586960586000,
    "Attributes": {
      "http.status_code": 500,
      "http.url": "http://example.com",
      "my.custom.application.tag": "hello"
    },
    "Resource": {
      "service.name": "donut_shop",
      "service.version": "semver:2.0.0",
      "k8s.pod.uid": "1138528c-c36e-11e9-a1a7-42010a800198"
    },
    "TraceId": "f4dbb3edd765f620",
    "SpanId": "43222c2d51a7abe3",
    "SeverityText": "INFO",
    "SeverityNumber": 9,
    "Body": {
      "i": "am",
      "an": "event",
      "of": {
        "some": "complexity"
      }
    }
  },
  {
    "Timestamp": 1586960586000,
    "Attributes": {
      "http.scheme": "https",
      "http.host": "donut.mycie.com",
      "http.target": "/order",
      "http.method": "post",
      "http.status_code": 500,
      "http.flavor": "1.1",
      "http.user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"
    }
  }
]
`
