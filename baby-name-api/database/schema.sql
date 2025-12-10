CREATE TABLE IF NOT EXISTS baby_names (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    year INTEGER NOT NULL,
    gender TEXT NOT NULL,
    count INTEGER NOT NULL,
    UNIQUE(name, year, gender)
);

CREATE INDEX IF NOT EXISTS idx_name ON baby_names(name);
