-- Tworzenie tabeli dla zreplikowanych paragonów
CREATE TABLE IF NOT EXISTS receipts (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) UNIQUE NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    data JSONB NOT NULL,
    replicated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tworzenie tabeli dla przechowywania stanu replikacji (offsetów)
CREATE TABLE IF NOT EXISTS replication_state (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    resume_token JSONB NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indeks na kolumnę JSONB w tabeli paragonów (wspiera szybkie wyszukiwanie)
CREATE INDEX IF NOT EXISTS idx_receipts_data ON receipts USING gin (data);
