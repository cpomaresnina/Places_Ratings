Mi extracción tenía 3.272 apariciones de negocios. El análisis terminó trabajando con 1.314 establecimientos distintos.

¿Se habían perdido datos? No: estaba contando entidades diferentes.

En mi proyecto final de Data Engineering, un negocio puede aparecer en varias búsquedas de Google Places: por cercanía a distintos puntos o por pertenecer a más de una categoría de búsqueda.

Si llevaba esas apariciones directamente al dashboard, algunos establecimientos podían pesar varias veces en los indicadores.

Por eso separé tres niveles:

• Respuesta de la API.
• Aparición de un establecimiento en una respuesta.
• Negocio consolidado por extracción.

Python para la ingesta, S3 para el landing y Spark SQL en Databricks para transformar los datos. Pero la decisión central fue definir qué representaba cada fila antes de calcular métricas.

Estoy documentando estas decisiones mientras avanzo con el proyecto. La próxima: por qué un conteo de reseñas ausente no debería convertirse automáticamente en cero.

¿Qué errores de granularidad te encontraste al trabajar con datos de APIs?

Repositorio: https://github.com/cpomaresnina/Places_Ratings

#DataEngineering #Databricks #SQL #AprenderHaciendo
