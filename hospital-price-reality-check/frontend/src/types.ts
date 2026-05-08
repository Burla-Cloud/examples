export type LineItem = {
  description?: string | null;
  unit?: string | null;
  gross_charge?: number | null;
  discounted_cash?: number | null;
  gross_charge_per_unit?: number | null;
  discounted_cash_per_unit?: number | null;
  dose?: string | null;
  hcpcs_billing_unit?: string | null;
  setting?: string | null;
};

export type StateBest = {
  name?: string | null;
  city?: string | null;
  median?: number | null;
  hospital_id?: string | null;
  mrf_url?: string | null;
  line_item?: LineItem | null;
};

export type RankedHospital = {
  hospital_id?: string | null;
  name?: string | null;
  city?: string | null;
  state?: string | null;
  median?: number | null;
  count?: number | null;
  mrf_url?: string | null;
  line_item?: LineItem | null;
};

export type CodeEntry = {
  category: string;
  code_system: string;
  code: string;
  display_name: string;
  what_it_is: string;
  when_youd_need_it: string;
  setting: string;
  bundled_with: string;
  not_bundled: string;
  tips: string[];
  stats: {
    count?: number;
    min?: number;
    max?: number;
    mean?: number;
    median?: number;
    p10?: number;
    p25?: number;
    p75?: number;
    p90?: number;
  };
  cheapest_in_state?: Record<string, StateBest>;
  priciest_in_state?: Record<string, StateBest>;
  top_cheapest?: RankedHospital[];
  top_priciest?: RankedHospital[];
  ranking_eligible_count?: number;
};

export type SpreadRow = {
  key: string;
  display_name: string;
  spread_ratio: number | null;
  dollar_spread: number;
  lowest: number;
  highest: number;
  median: number;
  mean?: number | null;
  category: string;
  count: number;
};

export type StateCodeStat = {
  median?: number | null;
  min?: number | null;
  max?: number | null;
  mean?: number | null;
  count?: number | null;
  p10?: number | null;
  p25?: number | null;
  p75?: number | null;
  p90?: number | null;
};

export type RunMeta = {
  hospitals_indexed?: number;
  hospitals_with_data?: number;
  observation_files?: number;
  unique_code_keys_in_reduce?: number;
  scale_summary?: {
    mode?: string;
    elapsed_seconds?: number;
    hospitals_submitted?: number;
    hospitals_succeeded?: number;
    hospitals_failed?: number;
    observation_rows_reported?: number;
    [k: string]: unknown;
  };
  data_source_note?: string;
};

export type HospitalIndexRow = {
  hospital_id: string;
  name?: string | null;
  state?: string | null;
  city?: string | null;
  ccn?: string | null;
  system?: string | null;
  latitude?: number | null;
  longitude?: number | null;
  mrf_url?: string | null;
  codes_covered?: number | null;
  honesty_score?: number | null;
};
