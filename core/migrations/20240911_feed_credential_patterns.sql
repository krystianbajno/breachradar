INSERT INTO credential_patterns (pattern) VALUES 
('(?P<username>[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}):(?P<password>.*)'),
('(?P<username>[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\|(?P<password>.*)');
