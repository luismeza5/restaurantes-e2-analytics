-- Bonus dataset · "Horas pico" (day-of-week x hour heatmap), powered by Spark's
-- analysis_peak_hours table. Useful as a 4th panel / staffing view.
SELECT day_of_week, day_name, hour, daypart, orders, revenue, avg_items
FROM dw.analysis_peak_hours
ORDER BY day_of_week, hour;
