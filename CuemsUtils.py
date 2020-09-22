class StringSanitizer():
    
    @staticmethod
    def sanitize(string):
        keepcharacters = ('.','_', '-')
        string = string.replace(' ', '_')
        return "".join(c for c in string if c.isalnum() or c in keepcharacters).rstrip().lower()