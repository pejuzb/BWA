import bcrypt as bc

def hash_password(password):
    salt = bc.gensalt()  # Generate salt
    hashed_password = bc.hashpw(password.encode('utf-8'), salt)  # Hash password
    return hashed_password


x = hash_password('lemon159')
print(x)

#$2b$12$u9f4geAtmWOU/qxMvzdhPO.juQFFxS3JGvToNhxIJ3UF3LowcCDLO

#$2b$12$mb4HtYMODe9meYvpAebxquPxDJIsAeytUVH54Yf8UZXSGMRhmcYxG
 #$2b$12$QwoqvzsACd9j6EzS6rTY3.F4hLM1XbFbK1O8VYBwI7R9hTT1gvgVm
