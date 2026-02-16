import { requireEnv } from "./env";

const API_KEY = requireEnv("CTA_BUS_TRACKER_API_KEY");
const BASE_URL = "https://www.ctabustracker.com/bustime/api/v3";

export type BusTimeResponse<T> = {
  "bustime-response": T;
};

const buildUrl = (endpoint: string, params: Record<string, string>) => {
  const url = new URL(`${BASE_URL}/${endpoint}`);
  url.searchParams.set("key", API_KEY);
  url.searchParams.set("format", "json");
  Object.entries(params).forEach(([key, value]) => {
    if (value) url.searchParams.set(key, value);
  });
  return url.toString();
};

export const busTimeRequest = async <T>(
  endpoint: string,
  params: Record<string, string>,
) => {
  const url = buildUrl(endpoint, params);
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`CTA Bus Tracker error ${res.status}`);
  }
  const payload = (await res.json()) as BusTimeResponse<T>;
  if (!payload["bustime-response"]) {
    throw new Error("CTA Bus Tracker response missing payload");
  }
  return payload["bustime-response"];
};
