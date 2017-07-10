
    
def get_cities(txt):
    city_country = []
    for t in txt:
        
        if " " in t:
            t = t.replace(" ","+")
        
        if t.isupper():
            country = t
        else:
            city = t

            city_country.append(city.encode("utf-8")+"+"+country)
    return city_country



if __name__ == "__main__":
    with open("400cities.txt") as f:
        txt = f.read().split("\n")

    o = ",".join(i for i in get_cities(txt))

    with open("400cities_final.txt","w") as f:
        f.write(o)
