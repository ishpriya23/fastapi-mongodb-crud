from fastapi import FastAPI, HTTPException, status, Query, Path 
from fastapi.encoders import jsonable_encoder
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from pymongo.errors import DuplicateKeyError

app = FastAPI(title="Employee API (FastAPI + MongoDB)")

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "assessment_db"
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
employees = db["employees"]

class EmployeeBase(BaseModel):
    name: Optional[str] = None
    department: Optional[str] = None
    salary: Optional[float] = None
    joining_date: Optional[str] = None
    skills: Optional[List[str]] = []

class EmployeeCreate(EmployeeBase):
    employee_id: str = Field(..., example="E123")
    name: str
    department: str
    salary: float
    joining_date: str
    skills: List[str] = []

class EmployeeUpdate(EmployeeBase):
    pass

@app.on_event("startup")
async def startup_db():
    schema = {
        "bsonType": "object",
        "required": ["employee_id", "name", "department", "salary", "joining_date", "skills"],
        "properties": {
            "employee_id": {
                "bsonType": "string",
                "description": "Must be a string and is required"
            },
            "name": {
                "bsonType": "string",
                "description": "Must be a string and is required"
            },
            "department": {
                "bsonType": "string",
                "description": "Must be a string"
            },
            "salary": {
                "bsonType": "double",
                "minimum": 0,
                "description": "Must be a positive number"
            },
            "joining_date": {
                "bsonType": "string",
                "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
                "description": "Must match YYYY-MM-DD"
            },
            "skills": {
                "bsonType": "array",
                "items": {"bsonType": "string"},
                "description": "Must be an array of strings"
            }
        }
    }

    if "employees" not in await db.list_collection_names():
        await db.create_collection("employees", validator={"$jsonSchema": schema})
    
    await employees.create_index("employee_id", unique=True)

def clean_doc(doc: dict) -> dict:
    doc = dict(doc)
    if "_id" in doc:
        doc["id"] = str(doc["_id"])
        doc.pop("_id", None)
    return doc

@app.get("/")
async def root():
    count = await employees.count_documents({})
    return {"message": "API running", "employees_count": count}

@app.get("/employees")
async def list_employees(
    department: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100)
):
    query = {}
    if department:
        query["department"] = department
    cursor = employees.find(query).sort("joining_date", -1).skip((page-1)*page_size).limit(page_size)
    results = []
    async for doc in cursor:
        results.append(clean_doc(doc))
    return results

@app.get("/employees/avg-salary")
async def avg_salary_by_department():
    pipeline = [
        {"$group": {"_id": "$department", "avg_salary": {"$avg": "$salary"}}},
        {"$project": {"department": "$_id", "avg_salary": "$avg_salary", "_id": 0}}
    ]
    cursor = employees.aggregate(pipeline)
    results = []
    async for doc in cursor:
        results.append(doc)
    return results

@app.get("/employees/search")
async def search_by_skill(
    skill: str = Query(..., min_length=1),
    page: int = 1,
    page_size: int = 10
):
    query = {"skills": {"$in": [skill]}}
    cursor = employees.find(query).skip((page-1)*page_size).limit(page_size)
    results = []
    async for doc in cursor:
        results.append(clean_doc(doc))
    return results

@app.post("/employees", status_code=status.HTTP_201_CREATED)
async def create_employee(emp: EmployeeCreate):
    doc = jsonable_encoder(emp)
    try:
        result = await employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=400, detail="employee_id must be unique")
    inserted = await employees.find_one({"_id": result.inserted_id})
    return clean_doc(inserted)

@app.put("/employees/{employee_id}")
async def update_employee(employee_id: str, emp: EmployeeUpdate):
    update_data = {k: v for k, v in emp.dict().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields provided for update")
    result = await employees.update_one({"employee_id": employee_id}, {"$set": update_data})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Employee not found")
    updated = await employees.find_one({"employee_id": employee_id})
    return clean_doc(updated)

@app.delete("/employees/{employee_id}")
async def delete_employee(employee_id: str):
    result = await employees.delete_one({"employee_id": employee_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Employee not found")
    return {"detail": "Employee deleted successfully"}

@app.get("/employees/{employee_id}")
async def get_employee(employee_id: str = Path(..., example="E123")):
    doc = await employees.find_one({"employee_id": employee_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Employee not found")
    return clean_doc(doc)
