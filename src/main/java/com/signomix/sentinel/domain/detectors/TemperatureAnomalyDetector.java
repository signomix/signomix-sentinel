package com.signomix.sentinel.domain.detectors;

import java.util.HashMap;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.iot.sentinel.SentinelConfig;
import com.signomix.sentinel.domain.ConditionResult;
import com.signomix.sentinel.domain.Measurement;
import com.signomix.sentinel.domain.SentinelCustomLogic;

import io.agroal.api.AgroalDataSource;

public class TemperatureAnomalyDetector implements SentinelCustomLogic {

    private Logger logger = Logger.getLogger(this.getClass());

    @Override
    public boolean hasMultipleMeasurements() {
        return true;
    }

    @Override
    public ConditionResult run(
            SentinelConfig config,
            String[] messageArray,
            int deviceRuleStatus,
            HashMap<String, String> scriptProperties,
            AgroalDataSource olapDs,
            List<Measurement> measurements) {

        ConditionResult result = new ConditionResult();
        // Read parameters
        String interal = scriptProperties.getOrDefault("interval", "1 minute");
        String duration = scriptProperties.getOrDefault("duration", "15 minutes");
        String statusTemeratures = scriptProperties.getOrDefault("statusTemperatures", "0:16,1:20,2:24,3:28");
        List<StatusTemperature> statusTemperatureList = new java.util.ArrayList<>();
        try {
            String[] pairs = statusTemeratures.split(",");
            for (String pair : pairs) {
                String[] parts = pair.split(":");
                int status = Integer.parseInt(parts[0]);
                double temperature = Double.parseDouble(parts[1]);
                statusTemperatureList.add(new StatusTemperature(status, temperature));
            }
        } catch (Exception e) {
            logger.error("Error parsing statusTemperatures: " + e.getMessage());
            result.error = true;
            result.errorMessage = "Error parsing statusTemperatures parameter.";
            return result;
        }
        if (measurements != null && measurements.size() > 0) {
            // use provided measurements
            result = detectAnomalies(recentReadings, deviceRuleStatus, statusTemperatureList, measurements);
            return result;
        } else {
            List<TemperatureReading> recentReadings = getReadingsLastNMinutes(messageArray[0], olapDs, interal,
                    duration);
            result = detectAnomalies(recentReadings, deviceRuleStatus, statusTemperatureList);
        }
        return result;
    }

    /**
     * Główna metoda do uruchamiania wszystkich sprawdzeń.
     * 
     * @param readings Lista odczytów z ostatniej doby, posortowana od najstarszego
     *                 do najnowszego.
     */
    public ConditionResult detectAnomalies(List<TemperatureReading> readings, int durationMinutes,
            List<StatusTemperature> statusTemperatures) {
        ConditionResult result = new ConditionResult();
        if (readings == null || readings.size() < 2) {
            result.errorMessage = "Not enough data for analysis";
            return result;
        }

        if (isOverheating(readings)) {
            result.violated = true;
            result.measurement = "temperature";
            result.errorMessage = "Current temperature exceeds the desired temperature and is continuously rising.";
            return result;
        }

        if (isRequiredTempIncorrect(readings, statusTemperatures)) {
            result.violated = true;
            result.measurement = "temperature_target";
            result.errorMessage = "Desired temperature is inconsistent with room status.";
            return result;
        }

        if (isNotTrendingToDesired(readings)) {
            result.violated = true;
            result.errorMessage = "Current temperature is not trending towards the desired temperature.";
            return result;
        }
        return result;
    }

    /**
     * Sprawdza, czy temperatura aktualna przekracza żądaną i rośnie od co najmniej
     * 30 minut.
     */
    private boolean isOverheating(List<TemperatureReading> recentReadings) {
        TemperatureReading latestReading = recentReadings.get(recentReadings.size() - 1);

        if (latestReading.getCurrentTemperature() <= latestReading.getRequiredTemperature()) {
            return false;
        }

        // Sprawdzenie, czy temperatura rośnie monotonicznie
        for (int i = 1; i < recentReadings.size(); i++) {
            if (recentReadings.get(i).getCurrentTemperature() < recentReadings.get(i - 1).getCurrentTemperature()) {
                return false; // Znaleziono spadek
            }
        }
        return true;
    }

    /**
     * Sprawdza, czy temperatura żądana jest niezgodna ze statusem, a status jest
     * stabilny od 30 minut.
     */
    private boolean isRequiredTempIncorrect(List<TemperatureReading> recentReadings,
            List<StatusTemperature> statusTemperatures) {
        int firstStatus = recentReadings.get(0).getRoomStatus();
        // Sprawdzenie, czy status był stały
        for (TemperatureReading reading : recentReadings) {
            if (reading.getRoomStatus() != firstStatus) {
                return false; // Status się zmienił w ciągu ostatnich 30 minut
            }
        }

        TemperatureReading latestReading = recentReadings.get(recentReadings.size() - 1);
        for (StatusTemperature st : statusTemperatures) {
            if (st.getStatus() == latestReading.getRoomStatus()) {
                if (latestReading.getRequiredTemperature() != st.getTemperature()) {
                    return true; // Temperatura żądana jest zgodna ze statusem
                }
            }
        }
        return false;
    }

    /**
     * Sprawdza, czy temperatura aktualna nie dąży do żądanej od co najmniej 30
     * minut.
     */
    private boolean isNotTrendingToDesired(List<TemperatureReading> recentReadings) {
        TemperatureReading latestReading = recentReadings.get(recentReadings.size() - 1);
        double currentTemp = latestReading.getCurrentTemperature();
        double desiredTemp = latestReading.getRequiredTemperature();

        // Obliczamy ogólny trend temperatury w okresie (regresja liniowa)
        double slope = calculateTemperatureTrend(recentReadings);

        // Sytuacja 1: Powinno być chłodniej, ale temperatura rośnie lub jest stała
        if (currentTemp > desiredTemp && slope >= 0) {
            return true;
        }

        // Sytuacja 2: Powinno być cieplej, ale temperatura spada lub jest stała
        if (currentTemp < desiredTemp && slope <= 0) {
            return true;
        }

        return false;
    }

    /**
     * Oblicza współczynnik nachylenia (trend) dla temperatury przy użyciu regresji
     * liniowej.
     * Wartość dodatnia oznacza trend wzrostowy, ujemna - spadkowy.
     */
    private double calculateTemperatureTrend(List<TemperatureReading> readings) {
        int n = readings.size();
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;

        for (int i = 0; i < n; i++) {
            // Używamy indeksu jako prostej osi X
            double x = i;
            double y = readings.get(i).getCurrentTemperature();
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumX2 += x * x;
        }

        if (n * sumX2 - sumX * sumX == 0) {
            return 0; // Brak trendu (pionowa linia, mało prawdopodobne)
        }

        // Wzór na współczynnik nachylenia (slope) w regresji liniowej
        return (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
    }

    private List<TemperatureReading> getReadingsLastNMinutes(String eui, AgroalDataSource olapDs, String interval,
            String duration) {
        String query = """
                SELECT
                  time_bucket_gapfill(?, tstamp) as day,
                  interpolate(avg(d1)) AS status,
                  avg(d2) AS temperature,
                  avg(d3) AS temp_target,
                  avg(d6) AS temp_req
                FROM
                  analyticdata
                WHERE eui=?
                AND tstamp>now() - interval ?
                AND tstamp<now()
                GROUP BY day
                ORDER BY day DESC;
                                """;
        List<TemperatureReading> readings = new java.util.ArrayList<>();
        try (java.sql.Connection conn = olapDs.getConnection();
                java.sql.PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setString(1, eui);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    java.sql.Timestamp ts = rs.getTimestamp("day");
                    int status = rs.getInt("status");
                    double temperature = rs.getDouble("temperature");
                    double tempTarget = rs.getDouble("temp_target");
                    double tempReq = rs.getDouble("temp_req");
                    readings.add(new TemperatureReading(ts, temperature, tempTarget, status, tempReq));
                }
            }
        } catch (java.sql.SQLException e) {
            e.printStackTrace();
            return new java.util.ArrayList<>();
        }
        return readings;
    }

}
