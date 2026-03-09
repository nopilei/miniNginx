import http from "k6/http";

export const options = {
  noVUConnectionReuse: false,
  scenarios: {
    high_rps: {
      executor: "constant-arrival-rate",
      rate: 11000,
      timeUnit: "1s",
      duration: "120s",
      preAllocatedVUs: 1000,
      maxVUs: 3000,
    },
  },
};

const URL = __ENV.TARGET_URL || "http://127.0.0.1:8080/events";
const payload = JSON.stringify({
  event_type: "order_created",
  data: {
    id: 1,
    user_id: 1,
    product_id: 1,
    amount: 1,
  },
});

export default function () {
  http.post(URL, payload, {
    headers: { "Content-Type": "application/json" },
  });
}
