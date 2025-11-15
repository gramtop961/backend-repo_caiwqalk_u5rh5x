from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# Mongo helpers are provided by the environment
from database import db, create_document, get_documents

app = FastAPI(title="Database Learning Platform API")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Schemas
class User(BaseModel):
    email: str
    name: str
    role: str = "learner"  # learner | admin
    avatar: Optional[str] = None
    created_at: Optional[datetime] = None

class Track(BaseModel):
    title: str
    slug: str
    description: str
    level: str = "beginner"
    tags: List[str] = []

class Lesson(BaseModel):
    track_slug: str
    title: str
    slug: str
    content: str  # markdown
    difficulty: str = "easy"
    order: int = 0

class Question(BaseModel):
    prompt: str
    options: List[str]
    answer_index: int
    explanation: Optional[str] = None

class Quiz(BaseModel):
    lesson_slug: str
    title: str
    questions: List[Question]

class Attempt(BaseModel):
    user_email: str
    quiz_id: str
    answers: List[int]
    score: Optional[int] = None
    created_at: Optional[datetime] = None


@app.get("/")
async def root():
    return {"status": "ok", "service": "db-learning-api"}

@app.get("/test")
async def test():
    try:
        await db.command({"ping": 1})
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Helper to idempotently seed if a document with slug doesn't exist
async def ensure_track(track: Track):
    existing = await get_documents("track", {"slug": track.slug}, limit=1)
    if not existing:
        await create_document("track", track.model_dump())

async def ensure_lesson(lesson: Lesson):
    existing = await get_documents("lesson", {"slug": lesson.slug}, limit=1)
    if not existing:
        await create_document("lesson", lesson.model_dump())

async def ensure_quiz(quiz: Quiz):
    existing = await get_documents("quiz", {"lesson_slug": quiz.lesson_slug}, limit=1)
    if not existing:
        await create_document("quiz", quiz.model_dump())


# Seed curriculum (SECTION I & II, Units 1-7)
@app.on_event("startup")
async def seed_data():
    try:
        # Tracks
        await ensure_track(Track(
            title="SECTION I",
            slug="section-i",
            description="Foundations: DB systems, relational model, and SQL fundamentals.",
            level="beginner",
            tags=["database", "relational", "sql"]
        ))
        await ensure_track(Track(
            title="SECTION II",
            slug="section-ii",
            description="Design, indexing, transactions, concurrency, and recovery.",
            level="intermediate",
            tags=["design", "indexing", "transactions"]
        ))

        # Lessons for SECTION I
        unit1_md = (
            "# UNIT 1: Introduction to Database Systems (07)\n\n"
            "## Overview of Database System Applications and Purpose\n"
            "- Why databases: persistence, concurrency, integrity, security, and scalability.\n"
            "- Applications: banking, e-commerce, healthcare, IoT, analytics.\n\n"
            "## Views of Data and Database Languages\n"
            "- Physical, logical, and view levels.\n"
            "- Languages: DDL, DML, DCL, TCL.\n\n"
            "## Relational Concepts and Design\n"
            "- Relations, attributes, tuples, schemas, constraints.\n"
            "- Integrity constraints: domain, key, referential.\n\n"
            "## Data Storage and Querying\n"
            "- Files, pages, buffer manager; query processing & optimization (overview).\n\n"
            "## Transaction Management & Architecture\n"
            "- ACID, logs, recovery, concurrency (high level).\n\n"
            "## Data Modeling with ER Model\n"
            "- Entities, attributes, relationships; cardinality and participation.\n"
            "- Constraints and keys: super, candidate, primary; weak entities.\n"
            "- Codd's rules (relational model principles).\n\n"
            "## Extended ER: Generalization & Aggregation\n"
            "- ISA hierarchies and aggregation.\n"
            "- Mapping ER to tables.\n"
        )

        unit2_md = (
            "# UNIT 2: Relational Data Model, Relational Algebra, and Calculus (07)\n\n"
            "## Structure & Schema\n- Relations, schemas, keys, foreign keys.\n\n"
            "## Relational Algebra\n- Selection (σ), Projection (π), Union (∪), Difference (−), Cartesian Product (×), Join (⨝).\n"
            "- Extended ops: rename, intersection, division, theta-join, natural join, outer joins.\n\n"
            "## Relational Calculus\n- Tuple Relational Calculus (TRC) and Domain Relational Calculus (DRC).\n"
        )

        unit3_md = (
            "# UNIT 3: Introduction to SQL (07)\n\n"
            "## SQL Overview\n- Role in DBMS, ANSI/ISO standards.\n\n"
            "## DDL & Basic Query Structure\n- CREATE, ALTER, DROP; SELECT-FROM-WHERE; ORDER BY; DISTINCT.\n\n"
            "## Operators & NULLs\n- Comparison, logical ops; set ops (UNION, INTERSECT, EXCEPT).\n- NULL semantics: IS NULL, COALESCE, three-valued logic.\n\n"
            "## Aggregates & Subqueries\n- COUNT, SUM, AVG, MIN, MAX; GROUP BY, HAVING; nested queries and IN/ANY/ALL/EXISTS.\n\n"
            "## Modifying Data\n- INSERT, UPDATE, DELETE; transactions and COMMIT/ROLLBACK.\n\n"
            "## Intermediate SQL\n- Joins, views, integrity constraints, data types, schema definition, authorization.\n\n"
            "## Advanced SQL\n- Embedding/integration (JDBC/ODBC), stored procedures, functions, triggers.\n"
        )

        # Lessons for SECTION II
        unit4_md = (
            "# UNIT 4: Relational Database Design (05)\n\n"
            "## Normalization & Good Design\n- Anomalies and decomposition goals.\n\n"
            "## Functional Dependencies & Normal Forms\n- 1NF, 2NF, 3NF, BCNF; synthesis vs. decomposition.\n\n"
            "## Multivalued Dependencies & 4NF\n- Rationale and design impact.\n\n"
            "## Design Process\n- Requirements → ER/Relational model → Normalization → Physical design.\n"
        )

        unit5_md = (
            "# UNIT 5: Indexing & Hashing (06)\n\n"
            "## Basics\n- Access paths, cost model, clustered vs. unclustered.\n\n"
            "## Ordered Indices\n- B-Tree, B+Tree structures; search, insert, delete.\n\n"
            "## Hashing\n- Static vs. dynamic hashing; extendible/linear hashing.\n\n"
            "## Others\n- Bitmap indices; multi-key access; defining indexes in SQL.\n"
        )

        unit6_md = (
            "# UNIT 6: Transaction Processing (06)\n\n"
            "## Concepts & Models\n- Transactions, atomicity, durability.\n\n"
            "## Isolation & ACID\n- Phenomena, isolation levels.\n\n"
            "## Storage, Schedules, Serializability, Recoverability\n- Conflict/view serializability; recoverable schedules.\n"
        )

        unit7_md = (
            "# UNIT 7: Concurrency Control & Recovery (07)\n\n"
            "## Concurrency Control\n- Lock-based protocols, deadlocks, multiple granularity, timestamp ordering.\n\n"
            "## Recovery Systems\n- Failure types, logging, ARIES-style recovery (high level), buffer management.\n"
        )

        # Create lessons
        lessons = [
            Lesson(track_slug="section-i", title="UNIT 1: Introduction to Database Systems", slug="unit-1-intro-db", content=unit1_md, difficulty="easy", order=1),
            Lesson(track_slug="section-i", title="UNIT 2: Relational Model, Algebra & Calculus", slug="unit-2-relational", content=unit2_md, difficulty="easy", order=2),
            Lesson(track_slug="section-i", title="UNIT 3: Introduction to SQL", slug="unit-3-sql", content=unit3_md, difficulty="easy", order=3),
            Lesson(track_slug="section-ii", title="UNIT 4: Relational Database Design", slug="unit-4-design", content=unit4_md, difficulty="intermediate", order=1),
            Lesson(track_slug="section-ii", title="UNIT 5: Indexing & Hashing", slug="unit-5-indexing", content=unit5_md, difficulty="intermediate", order=2),
            Lesson(track_slug="section-ii", title="UNIT 6: Transaction Processing", slug="unit-6-transactions", content=unit6_md, difficulty="intermediate", order=3),
            Lesson(track_slug="section-ii", title="UNIT 7: Concurrency Control & Recovery", slug="unit-7-concurrency", content=unit7_md, difficulty="intermediate", order=4),
        ]

        for ls in lessons:
            await ensure_lesson(ls)

        # Simple one-question quizzes per unit (can expand later)
        quizzes = [
            Quiz(lesson_slug="unit-1-intro-db", title="Unit 1 Quiz", questions=[
                Question(prompt="Which property is NOT part of ACID?", options=["Atomicity","Consistency","Isolation","Distribution"], answer_index=3, explanation="Durability is the D; Distribution is not an ACID property."),
            ]),
            Quiz(lesson_slug="unit-2-relational", title="Unit 2 Quiz", questions=[
                Question(prompt="Which operator in Relational Algebra filters rows?", options=["Projection (π)","Selection (σ)","Union (∪)","Join (⨝)"], answer_index=1),
            ]),
            Quiz(lesson_slug="unit-3-sql", title="Unit 3 Quiz", questions=[
                Question(prompt="Which SQL clause groups rows for aggregation?", options=["WHERE","GROUP BY","ORDER BY","HAVING"], answer_index=1),
            ]),
            Quiz(lesson_slug="unit-4-design", title="Unit 4 Quiz", questions=[
                Question(prompt="BCNF eliminates which issue most strongly?", options=["Update anomalies","Lost updates","Phantoms","Buffer thrashing"], answer_index=0),
            ]),
            Quiz(lesson_slug="unit-5-indexing", title="Unit 5 Quiz", questions=[
                Question(prompt="A B+Tree stores keys in which nodes?", options=["Leaves only","Internal only","Both internal and leaves","Neither"], answer_index=2),
            ]),
            Quiz(lesson_slug="unit-6-transactions", title="Unit 6 Quiz", questions=[
                Question(prompt="Read committed prevents which phenomenon?", options=["Dirty reads","Non-repeatable reads","Phantoms","Deadlocks"], answer_index=0),
            ]),
            Quiz(lesson_slug="unit-7-concurrency", title="Unit 7 Quiz", questions=[
                Question(prompt="Which protocol can cause deadlocks?", options=["Timestamp ordering","Two-phase locking","Optimistic CC","MVCC"], answer_index=1),
            ]),
        ]

        for q in quizzes:
            await ensure_quiz(q)
    except Exception as e:
        # Avoid crashing startup due to seed errors (e.g., transient DB)
        # You can check /test to verify DB connectivity after boot.
        print("[seed] Skipped seeding due to error:", e)


# Tracks
@app.get("/tracks")
async def list_tracks():
    items = await get_documents("track", {}, limit=100)
    return items

# Lessons
@app.get("/tracks/{slug}/lessons")
async def list_lessons(slug: str):
    items = await get_documents("lesson", {"track_slug": slug}, limit=200)
    items.sort(key=lambda x: x.get("order", 0))
    return items

@app.get("/lessons/{slug}")
async def get_lesson(slug: str):
    items = await get_documents("lesson", {"slug": slug}, limit=1)
    if not items:
        raise HTTPException(404, "Lesson not found")
    return items[0]

# Quizzes
@app.get("/lessons/{slug}/quiz")
async def get_quiz(slug: str):
    items = await get_documents("quiz", {"lesson_slug": slug}, limit=1)
    if not items:
        raise HTTPException(404, "Quiz not found")
    return items[0]

class SubmitAttempt(BaseModel):
    user_email: str
    answers: List[int]

@app.post("/quizzes/{quiz_id}/attempt")
async def submit_attempt(quiz_id: str, payload: SubmitAttempt):
    # Note: _id is stored as a string by helper, but if not found, return 404.
    quizzes = await get_documents("quiz", {"_id": quiz_id}, limit=1)
    if not quizzes:
        raise HTTPException(404, "Quiz not found")
    quiz = quizzes[0]
    correct = 0
    for i, ans in enumerate(payload.answers):
        if i < len(quiz["questions"]) and ans == quiz["questions"][i]["answer_index"]:
            correct += 1
    score = int((correct / max(1, len(quiz["questions"])))*100)
    attempt = Attempt(
        user_email=payload.user_email,
        quiz_id=quiz_id,
        answers=payload.answers,
        score=score,
        created_at=datetime.utcnow()
    )
    await create_document("attempt", attempt.model_dump())
    return {"score": score, "correct": correct, "total": len(quiz["questions"]) }


# Simple progress endpoint
@app.get("/users/{email}/progress")
async def get_progress(email: str):
    attempts = await get_documents("attempt", {"user_email": email}, limit=1000)
    by_quiz = {}
    for a in attempts:
        qid = a.get("quiz_id")
        if qid not in by_quiz or a.get("score", 0) > by_quiz[qid]:
            by_quiz[qid] = a.get("score", 0)
    completed = sum(1 for s in by_quiz.values() if s >= 70)
    return {"quizzes_completed": completed, "best_scores": by_quiz}
