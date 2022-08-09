from fastapi import APIRouter

router = APIRouter(prefix="/mappings")

### MAPPINGS ###
@router.get("/mappings/customers")
async def get_all_mappings_related_to_customers(): ...

@router.get("/mappings/customers/{customer_id}")
async def get_all_mappings_related_to_a_specific_customer(): ...

@router.get("/mappings/cities")
async def get_all_mappings_for_location_names(): ...

@router.get("/mappings/states")
async def get_all_mappings_for_location_names(): ...

@router.post("/mappings/customers")
async def create_new_mapping_for_a_customer(): ...

@router.post("/mappings/cities")
async def create_new_mapping_for_a_location(): ...

@router.post("/mappings/states")
async def create_new_mapping_for_a_location(): ...