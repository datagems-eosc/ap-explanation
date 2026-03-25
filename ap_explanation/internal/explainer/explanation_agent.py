from typing import List

from litellm import Message, completion

from .explainer import Explainer


class ExplanationAgent(Explainer):

    def __init__(self, api_base: str, api_key: str, model: str):
        self.api_base = api_base
        self.model = model
        self.api_key = api_key

    def _completion(self, messages: List[Message]) -> str:
        response = completion(
            api_base=self.api_base,
            api_key=self.api_key,
            model=self.model,
            messages=messages
        )
        return response.choices[0].message.content

    async def explain(self, query: str, provenance: str, database_schema: str) -> str:
        """Generate an explanation for a SQL query result based on the query, its provenance, and the database schema.
        Args:
            query: The SQL query that was executed.
            provenance: The provenance formula explaining which tuples contributed to the query result.
            database_schema: A description of the database schema, including tables, columns, and relationships.
        Returns: 
            A natural language explanation of why the query returned the specific result, using domain-specific language derived from the database schema.
        """

        user_query = "\n".join([
            f"Query: {query}",
            f"Provenance: {provenance}",
            f"Database Schema: {database_schema}"
        ])

        messages = [
            Message(role="system", content=EXPLANATION_PROMPT),
            Message(role="user", content=user_query)
        ]
        return self._completion(messages)


EXPLANATION_PROMPT = """

# System Prompt

You are an expert in **SQL query interpretation and database provenance**.

Your task is to explain **why a SQL query returned a specific result**, using **provenance information**.

You must generate explanations using **domain-specific language derived from the database schema**.

The explanations must be understandable by someone who understands the **application domain**, even if they do not understand SQL or provenance theory.

---

## Database Schema

You will be provided with a database schema that defines the structure of the data

Use the schema to interpret:

- table names
- column names
- relationships between tables

Always explain results using the **domain meaning of the schema**, not technical SQL terminology.

---

## Provenance Concepts

The provenance formula explains **which tuples contributed to producing the query result**.

Rules:

- Elements like `table@pXrY` refer to **specific tuples from a table**.
- The operator **⊗** indicates that tuples were **combined through joins** in the query.
- A formula represents **one derivation explaining why the result exists**.
- Multiple formulas mean that **multiple independent derivations produced the same result**.

The tuple data associated with each reference describes the **actual database records** involved.

Your task is to translate this provenance information into a **clear explanation using the schema vocabulary**.

---

## Output Structure

Your explanation must follow this structure:

### Query intent

Explain **what the query is trying to find**, using the meaning of the tables and attributes defined in the schema.

Structure:

The query asks for **[natural language explanation of the query using schema terms]**.

---

### Result explanation

Explain what the returned result represents in the domain.

Structure:

The result "`[RESULT]`" means that **[interpret the result using schema concepts]**.

---

### Provenance explanation

Explain **why this result appears**, based on the provenance formula.

Structure:

The provenance tells us that we obtained this result because:

For each derivation:

- Identify the tuples involved
- Explain what each tuple represents in the domain
- Explain how the tuples are connected through the relationships defined in the schema
- Explain how together they justify the result

If multiple derivations exist, explain that **multiple records independently support the same result**.

---

## Important Rules

- Always use **domain language derived from the schema**.
- Do **not repeat raw SQL syntax in the explanation**.
- Focus on **how the records contributed to producing the result**.
- Clearly explain **how the joined tuples lead to the returned value**.
- Prefer **clear natural explanations rather than technical database terminology**.
"""
