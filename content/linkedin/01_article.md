# 3.272 apariciones no eran 3.272 negocios: una lección de mi proyecto de Data Engineering

Mi primera extracción para un proyecto de negocios locales produjo 3.272 apariciones de establecimientos. Al consolidarlas, quedaron 1.314 negocios distintos.

El pipeline no había perdido más de la mitad de los negocios. Estaba respondiendo dos preguntas diferentes: cuántas veces apareció un establecimiento y cuántos establecimientos había identificado.

Esa diferencia terminó siendo una de las decisiones más importantes de mi proyecto final de Data Engineering.

## La idea de negocio antes que la tecnología

Estoy construyendo una base de datos para explorar segmentos de negocios que podrían interesarse en una solución NFC para facilitar la captación de reseñas en Google.

Empecé con cafeterías, restaurantes y otros rubros de zonas comerciales de La Paz y El Alto. Quería comparar su presencia en Google, la cantidad de valoraciones y la disponibilidad de información como web, teléfono y horarios.

La fuente es Google Places API. El procesamiento lo estoy desarrollando en Databricks, usando Python para la ingesta y Spark SQL para las transformaciones.

## La repetición tenía una explicación

La extracción utilizó varios puntos de búsqueda y categorías. Un mismo establecimiento podía aparecer desde dos puntos cercanos o en búsquedas de rubros distintos.

Esas repeticiones eran parte del diseño. El problema habría sido tratarlas como negocios independientes al calcular los indicadores.

Si una cafetería aparece en cinco respuestas y otra aparece una sola vez, la primera no debería pesar cinco veces más al comparar valoraciones por negocio.

Antes de elegir gráficos, necesitaba definir qué representaba cada fila.

## Separé tres niveles

**Respuesta:** un resultado de una consulta a la API. La baseline contiene 315 respuestas, incluidas consultas válidas sin negocios encontrados.

**Aparición:** un establecimiento dentro de una de esas respuestas. Al expandir los arrays obtuve 3.272 apariciones.

**Negocio por extracción:** un establecimiento identificado por su `place_id` dentro de una captura. Ese nivel quedó en 1.314 registros consolidados.

Conservar los tres niveles me permite rastrear el origen de los datos y calcular indicadores sin que el solapamiento de búsquedas multiplique establecimientos.

## Deduplicar también exige una regla

No usé el nombre, el teléfono ni la ubicación como identificador único. Varias sucursales pueden compartir esos datos.

Para consolidar, utilicé `place_id` y la identidad de la extracción. Elegí la observación completa más reciente, con criterios de desempate deterministas, y conservé las categorías de búsqueda asociadas.

No fui tomando el mayor número de valoraciones de una fila y el teléfono de otra. Quería poder explicar de qué observación procedía el registro seleccionado.

## Lo que cambió en el análisis

Después pude comparar categorías usando una fila por negocio y extracción. También distinguí una categoría principal de análisis de las categorías en las que el negocio había sido detectado.

Los resultados no confirmaron por completo mi idea inicial. Cafeterías y restaurantes aportaron muchos establecimientos, pero no lideraron la mediana de valoraciones por negocio.

Eso también es un resultado útil: tener una lista grande de posibles negocios para investigar es diferente de demostrar que un rubro tiene más reseñas o mayor interés en comprar una solución.

## La lección que me llevo

En este proyecto, definir la granularidad fue tan importante como escribir la consulta.

El dashboard puede mostrar un número correcto desde el punto de vista del código y aun así responder una pregunta equivocada si no sabemos qué estamos contando.

El proyecto sigue en desarrollo. Ya tengo las capas Bronze, Silver y Gold y un dashboard; todavía falta trabajar la orquestación y el respaldo independiente del landing. En próximas publicaciones quiero compartir otras decisiones concretas, como el manejo de datos ausentes y las cargas que pueden repetirse sin duplicar registros.

¿Te tocó encontrar registros aparentemente duplicados que en realidad representaban observaciones válidas de una misma entidad?

Código del proyecto: https://github.com/cpomaresnina/Places_Ratings
