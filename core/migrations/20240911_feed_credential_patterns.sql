INSERT INTO classifier_patterns (pattern, class) VALUES 
('(?P<username>[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}):(?P<password>.*)', 'CREDENTIAL'),
('(?P<username>[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\|(?P<password>.*)', 'CREDENTIAL');
