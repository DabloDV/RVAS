-- Schema and tables for local ETL target (idempotent)

CREATE SCHEMA IF NOT EXISTS healthtech;

-- Doctors dimension (doctors.xlsx)
CREATE TABLE IF NOT EXISTS healthtech.doctors (
  doctor_id   INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  specialty   TEXT
);

-- Appointments fact (appointments.xlsx)
CREATE TABLE IF NOT EXISTS healthtech.appointments (
  booking_id   INTEGER PRIMARY KEY,
  patient_id   INTEGER,
  doctor_id    INTEGER NOT NULL REFERENCES healthtech.doctors(doctor_id),
  booking_date DATE    NOT NULL,
  status       TEXT    NOT NULL CHECK (status IN ('confirmed','cancelled'))
);

-- Indexes for common filters and joins
CREATE INDEX IF NOT EXISTS idx_appointments_doctor_id
  ON healthtech.appointments(doctor_id);

CREATE INDEX IF NOT EXISTS idx_appointments_status
  ON healthtech.appointments(status);

CREATE INDEX IF NOT EXISTS idx_appointments_booking_date
  ON healthtech.appointments(booking_date);