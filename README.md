# Asistente Virtual

**Versión:** 0.2  
**Autor:** Joxan Portilla Hernández 

DESARROLLO PARA ALEGRE Y BRAVO 



---

## Descripción

El Consultor Virtual es un proyecto desarrollado en Python que integra agentes de inteligencia artificial para la gestión y análisis de información empresarial.  
Está diseñado con un enfoque modular y escalable, permitiendo la conexión con fuentes de datos (bases de datos, hojas de cálculo, APIs) y la orquestación de agentes inteligentes usando LangChain y LangGraph.

Su objetivo es asistir en tareas analíticas como:
- Consultas contables (CxC, CxP, indicadores financieros).
- Generación de reportes dinámicos.
- Análisis de KPIs con base en datos estructurados.
- Integración con modelos de lenguaje (LLMs) para interpretación contextual.

---

## Dependencias principales

El proyecto está gestionado mediante `pyproject.toml` y utiliza las siguientes bibliotecas principales:

| Librería | Propósito |
|-----------|------------|
| pandas | Manipulación y análisis de datos tabulares. |
| openpyxl | Lectura y escritura de archivos Excel (.xlsx). |
| jsonschema | Validación de estructuras JSON. |
| python-dateutil | Manejo avanzado de fechas. |
| pyyaml | Lectura y escritura de archivos YAML. |
| pydantic | Validación y modelado de datos. |
| requests | Comunicación con APIs externas. |
| langchain | Creación de cadenas y agentes de IA. |
| langchain-community | Extensiones y conectores adicionales. |
| langgraph | Orquestación de flujos de agentes con memoria. |

---

## Instalación

Sigue estos pasos para configurar el entorno de desarrollo:

1. Clona el repositorio  
   ```bash
   git clone https://github.com/joxanportilla/asistente-virtual.git
   cd asistente-virtual
2. Crea un entorno virtual
python -m venv venv
source venv/bin/activate       # macOS / Linux
venv\Scripts\activate          # Windows
3. Instala las dependecias 
pip install .


Para iniciar el asistente:

python -m app.main


O si el proyecto cuenta con una interfaz desarrollada en Streamlit u otra tecnología:

streamlit run app/main.py