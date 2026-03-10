-- Run this in Supabase SQL Editor (Database → SQL Editor → New Query)

-- ── Forecast snapshots table ─────────────────────────────────────────────────
CREATE TABLE forecast_snapshots (
  id               BIGSERIAL PRIMARY KEY,
  captured_at      TIMESTAMPTZ NOT NULL,
  forecast_date    DATE        NOT NULL,
  lead_days        INTEGER     NOT NULL,
  location         TEXT        NOT NULL,
  lat              FLOAT       NOT NULL,
  lon              FLOAT       NOT NULL,
  elev             INTEGER,
  fog_affinity     TEXT,
  model            TEXT        NOT NULL,   -- ecmwf | icon | gfs | metno
  time_window       TEXT        NOT NULL,   -- sunrise | sunset

  -- Forecast values
  cloud_total      FLOAT,
  cloud_low        FLOAT,
  cloud_mid        FLOAT,
  cloud_high       FLOAT,
  precip           FLOAT,
  visibility       FLOAT,
  windspeed        FLOAT,
  dewpoint         FLOAT,
  temperature      FLOAT,
  photo_score      INTEGER,
  fog_score        INTEGER,

  -- Actual observed values (filled in after the date passes)
  actual_fetched      BOOLEAN DEFAULT FALSE,
  actual_cloud_total  FLOAT,
  actual_cloud_low    FLOAT,
  actual_cloud_mid    FLOAT,
  actual_cloud_high   FLOAT,
  actual_precip       FLOAT,
  actual_visibility   FLOAT,
  actual_windspeed    FLOAT,
  actual_dewpoint     FLOAT,
  actual_temperature  FLOAT,
  actual_photo_score  INTEGER
);

-- ── Indexes for fast querying ─────────────────────────────────────────────────
CREATE INDEX idx_snapshots_date     ON forecast_snapshots (forecast_date);
CREATE INDEX idx_snapshots_location ON forecast_snapshots (location);
CREATE INDEX idx_snapshots_model    ON forecast_snapshots (model);
CREATE INDEX idx_snapshots_fetched  ON forecast_snapshots (actual_fetched);

-- ── Allow public read access (needed for the dashboard HTML) ─────────────────
ALTER TABLE forecast_snapshots ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read" ON forecast_snapshots
  FOR SELECT USING (true);

CREATE POLICY "Service insert" ON forecast_snapshots
  FOR INSERT WITH CHECK (true);

CREATE POLICY "Service update" ON forecast_snapshots
  FOR UPDATE USING (true);

-- ── Accuracy summary view (pre-computed for dashboard charts) ────────────────
CREATE VIEW accuracy_summary AS
SELECT
  model,
  location,
  fog_affinity,
  time_window,
  lead_days,
  COUNT(*)                                          AS sample_count,
  ROUND(AVG(ABS(photo_score - actual_photo_score))::NUMERIC, 1)  AS avg_photo_error,
  ROUND(AVG(ABS(cloud_low   - actual_cloud_low))::NUMERIC, 1)    AS avg_cloud_low_error,
  ROUND(AVG(ABS(visibility  - actual_visibility) / 1000)::NUMERIC, 2) AS avg_vis_error_km,
  ROUND(AVG(ABS(temperature - actual_temperature))::NUMERIC, 2)  AS avg_temp_error,
  ROUND(CORR(photo_score, actual_photo_score)::NUMERIC, 3)        AS photo_score_correlation
FROM forecast_snapshots
WHERE actual_fetched = TRUE
  AND actual_photo_score IS NOT NULL
GROUP BY model, location, fog_affinity, time_window, lead_days
ORDER BY model, location, lead_days;
