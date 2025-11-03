-- Doctor with the highest number of confirmed appointments
SELECT d.doctor_id, d.name, COUNT(*) AS confirmed_appointments
FROM healthtech.appointments a
JOIN healthtech.doctors d USING (doctor_id)
WHERE a.status = 'confirmed'
GROUP BY d.doctor_id, d.name
ORDER BY confirmed_appointments DESC
LIMIT 1;

-- Confirmed appointments for patient_id = 34
SELECT COUNT(*) AS confirmed_for_patient_34
FROM healthtech.appointments
WHERE status = 'confirmed'
  AND patient_id = 34;

-- Cancelled appointments between 2025-10-21 and 2025-10-24 (inclusive)
SELECT COUNT(*) AS cancelled_between_dates
FROM healthtech.appointments
WHERE status = 'cancelled'
  AND booking_date BETWEEN DATE '2025-10-21' AND DATE '2025-10-24';

-- Total confirmed appointments grouped by doctor
SELECT d.doctor_id, d.name, COUNT(*) AS confirmed_total
FROM healthtech.appointments a
JOIN healthtech.doctors d USING (doctor_id)
WHERE a.status = 'confirmed'
GROUP BY d.doctor_id, d.name
ORDER BY d.doctor_id;