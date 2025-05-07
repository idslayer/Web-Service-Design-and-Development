from itertools import product
from fastapi import FastAPI, HTTPException, Depends, status
from pydantic import BaseModel , EmailStr, field_validator
from typing import List, Optional, Set
from dbConn import conn
from auth import AuthHandler
from schemas import AuthDetails
from fastapi import Query
from enum import Enum
from contextlib import asynccontextmanager
@asynccontextmanager
async def lifespan(app: FastAPI):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE OR REPLACE VIEW rented_films_24h AS
        SELECT 
            f.film_id, 
            f.title, 
            COUNT(r.rental_id) AS times_rented
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        WHERE r.rental_date >= NOW() - INTERVAL 1 DAY
        GROUP BY f.film_id, f.title
        ORDER BY times_rented DESC
    """)
    cursor.close()
    yield

app = FastAPI(lifespan=lifespan)
# app = FastAPI()


class Products(BaseModel):
    ProductID: int
    Name: str
class ProductQuantities(BaseModel):
    ProductID: int
    Name: str
    ProductNumber: str
    TotalQuantity: int
    SafetyStockLevel: int
    ReorderPoint: int
    StandardCost: float
    ListPrice: float
class EmployeePay(BaseModel):
    BusinessEntityID: int
    NationalIDNumber: int
    Name: Optional[str] = None
    OrganizationLevel: Optional[int] = None
    JobTitle: Optional[str] = None
    Rate: float
    PayFrequency:int

class Citys(BaseModel):
    CityID: int
    CityName: str
    CountryID: int

class Customers(BaseModel):
    CustomerID: int
    CustomerName: str
    CustomerAddress: str
    CustomerPhone: str
    CustomerCity: str
    CustomerCountry: str

#enum
class RatingEnum(str, Enum):
    G = "G"
    PG = "PG"
    PG_13 = "PG-13"
    R = "R"
    NC_17 = "NC-17"


class SpecialFeaturesEnum(str, Enum):
    Trailers = "Trailers"
    Commentaries = "Commentaries"
    Deleted_Scenes = "Deleted Scenes"
    Behind_the_Scenes = "Behind the Scenes"

#auth
auth_handler = AuthHandler()
users = []
@app.post('/register', tags=["Auth"], status_code=201)
def register(auth_details: AuthDetails):
    if any(x['username'] == auth_details.username for x in users):
        raise HTTPException(status_code=400, detail='Username is taken')
    hashed_password = auth_handler.get_password_hash(auth_details.password)
    users.append({
    'username': auth_details.username,
    'password': hashed_password
    })
    return
@app.post('/login', tags=["Auth"])
def login(auth_details: AuthDetails):
    user = None
    for x in users:
        if x['username'] == auth_details.username:
            user = x
            break
    if (user is None) or (not auth_handler.verify_password(auth_details.password,
user['password'])):
        raise HTTPException(status_code=401, detail='Invalid username and/or password')
    token = auth_handler.encode_token(user['username'])
    return { 'token': token }


# get customer from view
@app.get("/customers/search",tags=["Customers"], response_model=List[Customers])
def search_customers(
    address: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    country: Optional[str] = Query(None)
):
    cursor = conn.cursor()
    base_query = "SELECT ID, name, address, phone, city, country FROM customer_list"
    conditions = []
    values = []

    if address:
        conditions.append("address LIKE %s")
        values.append(f"%{address}%")
    if city:
        conditions.append("city LIKE %s")
        values.append(f"%{city}%")
    if country:
        conditions.append("country LIKE %s")
        values.append(f"%{country}%")

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    base_query += " LIMIT 50"
    cursor.execute(base_query, tuple(values))
    customers = cursor.fetchall()
    cursor.close()

    return [
        Customers(
            CustomerID = customer[0],
            CustomerName = customer[1],
            CustomerAddress = customer[2],
            CustomerPhone = customer[3],
            CustomerCity = customer[4],
            CustomerCountry = customer[5]
        ) for customer in customers
    ]

# Get data of 50 film rented in the last 24 hours from view
class RentedFilmInfo(BaseModel):
    film_id: int
    title: str
    times_rented: int


@app.get("/films/rented-last-24h", tags=["Films"], response_model=List[RentedFilmInfo])
def get_rented_films():
    cursor = conn.cursor()
    query = "SELECT film_id, title, times_rented FROM rented_films_24h LIMIT 50"
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()

    return [
        RentedFilmInfo(film_id=row[0], title=row[1], times_rented=row[2])
        for row in rows
    ]


class FilmList(BaseModel):
    FilmID: int
    Title: str
    Description: str
    Category: str
    Price: float
    Length: int
    Rating: str
    Actors: str

# The GET endpoint will retrieve all films by category in view film_list.
@app.get("/films/by-category", tags=["Films"], response_model=List[FilmList])
def get_films_by_category(category: str):
    cursor = conn.cursor()
    query = """
    SELECT FID, title, description, category, price, length, rating, actors 
    FROM film_list 
    WHERE category = %s 
    LIMIT 50
    """
    cursor.execute(query, (category,))
    films = cursor.fetchall()
    cursor.close()
    if not films:
        raise HTTPException(status_code=404, detail="No films found in this category")
    return [
        FilmList(
            FilmID = film[0],
            Title = film[1],
            Description = film[2],
            Category = film[3],
            Price = film[4],
            Length = film[5],
            Rating = film[6],
            Actors = film[7]
        ) for film in films
    ]


class CustomerCreate(BaseModel):
    store_id: int
    first_name: str
    last_name: str
    email: str
    address_id: int

# The POST endpoint create new customer 
@app.post("/customers/new", tags=["Customers"])
def create_customer(customer: CustomerCreate, username: str = Depends(auth_handler.auth_wrapper)):
    cursor = conn.cursor()

    # Check store_id and address_id 
    cursor.execute("SELECT store_id FROM store WHERE store_id = %s", (customer.store_id,))
    if not cursor.fetchone():
        raise HTTPException(status_code=400, detail="Invalid store_id")

    cursor.execute("SELECT address_id FROM address WHERE address_id = %s", (customer.address_id,))
    if not cursor.fetchone():
        raise HTTPException(status_code=400, detail="Invalid address_id")

    # Query insert new customer into database
    query = """
        INSERT INTO customer ( store_id, first_name, last_name, email, address_id, create_date)
        VALUES ( %s, %s, %s, %s, %s, NOW())
    """
    cursor.execute(query, (
        customer.store_id,
        customer.first_name,
        customer.last_name,
        customer.email,
        customer.address_id
    ))
    conn.commit()
    customer_id = cursor.lastrowid
    cursor.close()

    return {
        "message": "Customer created successfully",
        "created_by": username,
        "customer_id": customer_id
    }

class CustomerUpdate(BaseModel):
    first_name: str
    last_name: str
    email: str
    address_id: int
    active: bool

# The PUT endpoint update customer
@app.put("/customers/update/{customer_id}", tags=["Customers"])
def update_customer(customer_id: int, customer: CustomerUpdate, username: str = Depends(auth_handler.auth_wrapper)):
    cursor = conn.cursor()
    
    cursor.execute("SELECT customer_id FROM customer WHERE customer_id = %s", (customer_id,))
    if cursor.fetchone() is None:
        raise HTTPException(status_code=404, detail="Customer not found")

    try:
        query = """
            UPDATE customer
            SET first_name=%s, last_name=%s, email=%s, address_id=%s, active=%s, last_update=NOW()
            WHERE customer_id=%s
        """
        cursor.execute(query, (customer.first_name, customer.last_name, customer.email, customer.address_id, customer.active, customer_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        cursor.close()

    return {"message": "Customer updated successfully", 
            "updated_by": username
    }

# The DELETE endpoint delete customer
@app.delete("/customers/delete/{customer_id}", tags=["Customers"])
def delete_customer(customer_id: int, username: str = Depends(auth_handler.auth_wrapper)):
    cursor = conn.cursor()
    query = "SELECT customer_id FROM sakila.customer WHERE customer_id = %s;"
    cursor.execute(query, (customer_id,))
    item = cursor.fetchone()
    if item is None:
        raise HTTPException(status_code=400, detail="Customer not found")

    query = "DELETE FROM sakila.customer WHERE customer_id = %s;"
    cursor.execute(query, (customer_id,))
    conn.commit()
    cursor.close()
    raise HTTPException(status_code=200, detail="Customer has been deleted")


class ActorCreate(BaseModel):
    first_name: str
    last_name: str


# The POST endpoint add new actor 
@app.post("/actors/new", tags=["Actors"])
def create_actor(actor: ActorCreate, username: str = Depends(auth_handler.auth_wrapper)):
    #Validate the input
    if not actor.first_name.strip() or not actor.last_name.strip():
        raise HTTPException(status_code=400, detail="First and last names cannot be empty")
    cursor = conn.cursor()

    
    query = """
        INSERT INTO actor (first_name, last_name)
        VALUES (%s, %s)
    """
    cursor.execute(query, (actor.first_name, actor.last_name))
    conn.commit()


    actor_id = cursor.lastrowid
    cursor.close()

    return {
        "message": "Actor created successfully",
        "created_by": username,
        "actor_id": actor_id,
        "actor_name": f"{actor.first_name} {actor.last_name}"
    }



# The PUT endpoint update actor
class ActorUpdate(BaseModel):
    first_name: str
    last_name: str

@app.put("/actors/update/{actor_id}", tags=["Actors"])
def update_actor(actor_id: int, actor: ActorUpdate, username: str = Depends(auth_handler.auth_wrapper)):
    if not actor.first_name.strip() or not actor.last_name.strip():
        raise HTTPException(status_code=400, detail="First and last names cannot be empty")

    cursor = conn.cursor()
    cursor.execute("SELECT actor_id FROM actor WHERE actor_id = %s", (actor_id,))
    if cursor.fetchone() is None:
        raise HTTPException(status_code=404, detail="Actor not found")

    try:
        query = "UPDATE actor SET first_name=%s, last_name=%s, last_update=NOW() WHERE actor_id=%s"
        cursor.execute(query, (actor.first_name, actor.last_name, actor_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        cursor.close()

    return {"message": "Actor updated successfully", "updated_by": username}



# The DELETE endpoint delete actor 
@app.delete("/actors/delete/{actor_id}", tags=["Actors"])
def delete_actor(actor_id: int, username: str = Depends(auth_handler.auth_wrapper)):
    cursor = conn.cursor()
    query = "SELECT actor_id FROM sakila.actor WHERE actor_id = %s;"
    cursor.execute(query, (actor_id,))
    item = cursor.fetchone()
    if item is None:
        raise HTTPException(status_code=400, detail="Actor not found")

    query = "DELETE FROM sakila.actor WHERE actor_id = %s;"
    cursor.execute(query, (actor_id,))
    conn.commit()
    cursor.close()
    raise HTTPException(status_code=200, detail="Actor has been deleted")


class FilmCreate(BaseModel):
    title: str
    description: Optional[str]
    release_year: int = 2025
    language_id: int
    rental_duration: int = 3
    rental_rate: float = 4.99
    length: Optional[int]
    replacement_cost: float = 19.99
    rating: RatingEnum
    special_features: Optional[Set[SpecialFeaturesEnum]] = set()

#validate special_features
@field_validator("special_features", mode="before")
@classmethod
def validate_special_features(cls, v):
    allowed = {e.value for e in SpecialFeaturesEnum}
    if v is None:
        return set()
    if isinstance(v, str):
        parsed = {i.strip().title() for i in v.split(",")}
    elif isinstance(v, (list, set)):
        parsed = {str(i).strip().title() for i in v}
    else:
        raise ValueError("Invalid type for special_features")

    for item in parsed:
        if item not in allowed:
            raise ValueError(
                f"Invalid special_feature: '{item}'. Allowed: {sorted(allowed)}"
            )
    return {SpecialFeaturesEnum(item) for item in parsed}


@app.post("/films/new", tags=["Films"])
def create_film(film: FilmCreate, username: str = Depends(auth_handler.auth_wrapper)):
    cursor = conn.cursor()

   # Check language
    query_check_language = "SELECT language_id FROM language WHERE language_id = %s"
    cursor.execute(query_check_language, (film.language_id,))
    language_exists = cursor.fetchone()
    # check features
    features_str = ','.join(film.special_features) if film.special_features else None

    if not language_exists:
        cursor.close()
        raise HTTPException(status_code=400, detail="Language ID does not exist")

    # Because the film table has special data types such as foreign keys language_id, enum rating and set special_features, we need to catch additional errors in the database.
    try:
        query = """
            INSERT INTO film 
            (title, description, release_year, language_id, 
            rental_duration, rental_rate, length, replacement_cost, rating, special_features)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            film.title,
            film.description,
            film.release_year,
            film.language_id,
            film.rental_duration,
            film.rental_rate,
            film.length,
            film.replacement_cost,
            film.rating,
            features_str
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
            
       
    film_id = cursor.lastrowid
    cursor.close()

    return {
        "message": "Film created successfully",
        "film_id": film_id,
        "created_by": username
    }


class FilmUpdate(BaseModel):
    title: str
    description: str
    release_year: int = 2025
    language_id: int
    length: int
    rating: RatingEnum
    special_features: Optional[Set[SpecialFeaturesEnum]] = set()

# The PUT endpoint update film
@app.put("/films/update/{film_id}", tags=["Films"])
def update_film(film_id: int, film: FilmUpdate, username: str = Depends(auth_handler.auth_wrapper)):
    cursor = conn.cursor()
    
    # Check film ID
    cursor.execute("SELECT film_id FROM film WHERE film_id = %s", (film_id,))
    if cursor.fetchone() is None:
        raise HTTPException(status_code=404, detail="Film not found")

    # Check language
    cursor.execute("SELECT language_id FROM language WHERE language_id = %s", (film.language_id,))
    if cursor.fetchone() is None:
        raise HTTPException(status_code=400, detail="Invalid language ID")
    # Check features
    features_str = ','.join(film.special_features) if film.special_features else None
    
    # Because the film table has special data types such as foreign keys language_id, enum rating and set special_features, we need to catch additional errors in the database.
    try:
        query = """
            UPDATE film SET title=%s, description=%s, release_year=%s,
            language_id=%s, length=%s, rating=%s, special_features=%s,
            last_update=NOW()
            WHERE film_id=%s
        """
        cursor.execute(query, (
            film.title, film.description, film.release_year,
            film.language_id, film.length, film.rating,
            features_str, film_id
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        cursor.close()

    return {"message": "Film updated successfully", "updated_by": username}

# The DELETE endpoint delete film
@app.delete("/films/delete/{film_id}", tags=["Films"])
def delete_film(film_id: int, username: str = Depends(auth_handler.auth_wrapper)):
    cursor = conn.cursor()
    query = "SELECT film_id FROM sakila.film WHERE film_id = %s;"
    cursor.execute(query, (film_id,))
    item = cursor.fetchone()
    if item is None:
        raise HTTPException(status_code=400, detail="Film not found")

    query = "DELETE FROM sakila.film WHERE film_id = %s;"
    cursor.execute(query, (film_id,))
    conn.commit()
    cursor.close()
    raise HTTPException(status_code=200, detail="Film has been deleted")


