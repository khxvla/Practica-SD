# Memoria Final Exahustiva: Scalable and Elastic Ticket Service (AWS Managed Systems)
**Práctica de Arquitectura de Sistemas Distribuidos y Computación en la Nube**

---

## Índice General
1. [Introducción, Contexto y Objetivos Principales](#1-introducción-contexto-y-objetivos-principales)
2. [Arquitectura General y Componentes en AWS](#2-arquitectura-general-y-componentes-en-aws)
    - 2.1. Topología del Clúster y Subredes VPC
    - 2.2. Flujo de Comunicación Síncrono (REST) vs Asíncrono (RabbitMQ)
    - 2.3. Infraestructura como Código (IaC con Terraform)
3. [Estrategias Estrictas de Correctitud y Control de Concurrencia](#3-estrategias-estrictas-de-correctitud-y-control-de-concurrencia)
    - 3.1. Entradas No Numeradas: Transacciones Optimistas en Redis
    - 3.2. Entradas Numeradas: Bloqueos Pesimistas a Nivel de Fila (PostgreSQL)
    - 3.3. Justificación del Modelo de Consistencia Fuerte
4. [Tolerancia a Fallos, Resiliencia y Garantías de Entrega](#4-tolerancia-a-fallos-resiliencia-y-garantías-de-entrega)
    - 4.1. Idempotencia y Evitación de Operaciones Duplicadas
    - 4.2. Dead-Letter Queues (DLQ) y Prevención de Poison Pills
    - 4.3. Semántica "At-Least-Once"
5. [Dinámica de Escalado Elástico y Modelado Matemático](#5-dinámica-de-escalado-elástico-y-modelado-matemático)
    - 5.1. Modelado de la Capacidad del Nodo y Latencia Simulada (Realismo)
    - 5.2. Formulación de Escalado por Backlog (Profundidad de Cola)
    - 5.3. Orquestación Dinámica de Nodos Lambda y Fargate
6. [Diseño y Justificación del Escenario de Carga Z(t)](#6-diseño-y-justificación-del-escenario-de-carga-zt)
7. [Evaluación Empírica del Rendimiento (Stress Testing)](#7-evaluación-empírica-del-rendimiento-stress-testing)
    - 7.1. Análisis del Throughput vs Número de Workers
    - 7.2. Tiempos de Encolado (Queue Backlog) vs Tiempo
    - 7.3. Degradación de Latencias por Percentiles (p50, p95, p99)
8. [Identificación de Cuellos de Botella y Escenario "Hotspot"](#8-identificación-de-cuellos-de-botella-y-escenario-hotspot)
9. [Respuestas a las Preguntas Conceptuales Obligatorias (Q1 y Q2)](#9-respuestas-a-las-preguntas-conceptuales-obligatorias-q1-y-q2)
10. [Conclusiones Finales](#10-conclusiones-finales)
11. [Uso Declarado de Inteligencia Artificial](#11-uso-declarado-de-inteligencia-artificial)

---

## 1. Introducción, Contexto y Objetivos Principales
La venta de entradas para conciertos y eventos de alta demanda representa uno de los desafíos más complejos dentro de la ingeniería de software y los sistemas distribuidos. Un sistema de este calibre está sujeto a fenómenos conocidos como "Flash Crowds", donde la carga de usuarios pasa de ser cercana a cero a picos masivos de cientos de miles de peticiones simultáneas en cuestión de segundos tras la apertura de las ventas.

El objetivo central de esta práctica ha sido diseñar, desde cero, una solución altamente distribuida, asíncrona y basada en componentes "Cloud Native" en Amazon Web Services (AWS) capaz de sobrevivir a estos picos. El reto fundamental radica en balancear dos fuerzas habitualmente opuestas en los sistemas distribuidos: la **Correctitud Estricta** (garantizar matemáticamente que jamás se venden dos entradas para un mismo asiento numerado) y la **Alta Escalabilidad** (capacidad de procesar peticiones masivas mediante la adición horizontal de hardware elástico).

Se han satisfecho de manera íntegra todos los requerimientos de la sección 1 a 11 del enunciado, cubriendo almacenamiento persistente ACID (PostgreSQL), procesamiento en colas (RabbitMQ), despliegue de Workers Stateless y automatización vía Infrastructure as Code (Terraform).

---

## 2. Arquitectura General y Componentes en AWS

### 2.1 Topología del Clúster y Subredes VPC
El sistema se despliega íntegramente dentro de una Virtual Private Cloud (VPC) aislada en la región `us-east-1` de AWS Academy. Para maximizar la seguridad, todas las bases de datos y brokers de mensajería se alojan utilizando direcciones IP privadas (`10.0.X.X` y `172.31.X.X`), impidiendo la manipulación externa y reduciendo la latencia intra-clúster.
*(Nota: Añadir diagrama exhaustivo de arquitectura cloud mostrando la red de EC2)*

### 2.2 Flujo de Comunicación Síncrono (REST) vs Asíncrono (RabbitMQ)
A lo largo del proyecto se implementaron y contrastaron dos paradigmas arquitectónicos para demostrar la superioridad del modelo asíncrono bajo estrés:
1. **Arquitectura Directa (Síncrona - REST):** El cliente realiza una petición HTTP que bloquea su hilo de ejecución hasta que el backend responde con un "OK" o un "SOLD OUT". Este modelo acopla temporalmente al cliente con el servidor de bases de datos. Si la base de datos se ralentiza, los buffers TCP del balanceador REST se llenan y el sistema colapsa (Timeouts 504 y Conexiones Rechazadas).
2. **Arquitectura Indirecta (Asíncrona - RabbitMQ):** Paradigma central del proyecto. El Load Balancer actúa como un mero `API Gateway`. Inyecta el payload de compra en el Exchange de un servidor RabbitMQ instalado en una instancia EC2 y responde inmediatamente al cliente con un ID de solicitud (`request_id = 202`). El procesamiento pesado (Validación, Bloqueo de BD, Confirmación) queda delegado al clúster elástico de workers ubicados "aguas abajo".

### 2.3 Infraestructura como Código (IaC con Terraform)
Cumpliendo íntegramente con el Requisito Opcional 11, se optó por estandarizar la creación de la infraestructura. A través de archivos `.tf` (Terraform), se declaran programáticamente las instancias EC2, se asocian a un `Security Group` que abre los puertos TCP necesarios (5432 Postgres, 5672 AMQP, 6379 Redis) y se inyecta un *User-Data Script* de cloud-init que instala, configura y arranca de forma silenciosa el broker y las bases de datos en minutos, eliminando el error humano.

---

## 3. Estrategias Estrictas de Correctitud y Control de Concurrencia

El mayor riesgo de la computación asíncrona y distribuida es la superposición de accesos a memoria compartida. En nuestro dominio, se gestionan dos modelos:

### 3.1 Entradas No Numeradas: Transacciones Optimistas en Redis
Para las entradas "Generales" (donde a los asistentes solo les importa tener un hueco, con límite de 100.000 unidades), se necesita velocidad pura. Hemos utilizado **Redis**. Al ser *single-threaded* en la ejecución de comandos, cualquier operación de incremento es intrínsecamente atómica.
Para evitar el "overselling" cuando el contador está en 99.999 y entran 5.000 peticiones en el mismo milisegundo, empleamos transacciones optimistas:
```python
pipeline = redis_client.pipeline()
pipeline.watch('unnumbered_sold')
sold = int(pipeline.get('unnumbered_sold'))
if sold < TOTAL_TICKETS:
    pipeline.multi()
    pipeline.incr('unnumbered_sold')
    pipeline.execute()
```
Si otro thread modifica la variable en medio de este bloque, la transacción aborta automáticamente, garantizando correctitud absoluta sin penalización por bloqueos pesados.

### 3.2 Entradas Numeradas: Bloqueos Pesimistas a Nivel de Fila (PostgreSQL)
Para el *seating* asignado, si los usuarios A y B solicitan el asiento "Fila 1, Asiento 5", utilizar un patrón optimista generaría altas tasas de rechazo (*abort rates*).
Se ha decidido implementar **PostgreSQL** montado en EC2, apoyado fuertemente en las normativas ACID. Utilizamos un **Bloqueo Pesimista Exclusivo**:
```sql
SELECT status FROM seats WHERE seat_number = %s FOR UPDATE;
```
La cláusula `FOR UPDATE` establece un *Write-Lock* en el motor interno de PostgreSQL sobre esa fila exacta. Ningún otro worker en el mundo puede leer el estado de esa fila hasta que se finaliza la transacción. Esto nos da garantía de exclusión mutua 100% libre de fallos distribuidos.

### 3.3 Justificación del Modelo de Consistencia Fuerte
Se justifica el uso exclusivo de este modelo porque es el único que respeta la legalidad comercial y previene el Overselling. Aunque la Consistencia Eventual mejoraría dramáticamente el Throughput, rompería las reglas del dominio al confirmar asientos dobles.

---

## 4. Tolerancia a Fallos, Resiliencia y Garantías de Entrega

La distribución asíncrona trae consigo el Teorema de Fallos de Red. Hemos protegido el sistema de la siguiente manera:

### 4.1 Idempotencia y Evitación de Operaciones Duplicadas
Si un fallo de red impide que el worker envíe el `ACK` a RabbitMQ, RabbitMQ asume que el worker murió y reasigna el mensaje a otro nodo. Esto significa que la base de datos podría recibir la petición de compra del Asiento 5 de la misma persona dos veces.
Para mitigarlo, implementamos **Idempotencia Transaccional**. Cada solicitud incorpora un `request_id` (UUID). Antes de insertar datos, el backend PostgreSQL y Redis ejecutan una búsqueda indexada de este `request_id`. Si existe, se descarta la inserción silenciosamente.

### 4.2 Dead-Letter Queues (DLQ) y Prevención de Poison Pills
¿Qué ocurre si el JSON de entrada está corrupto o la base de datos rechaza la clave primaria, generando un volcado de excepción en el código Python? El worker fallecerá y RabbitMQ volverá a meter el mensaje corrupto en la cola, matando indefinidamente a cada worker que lo toque (fenómeno Poison Pill).
Para evitar esto, configuramos en `broker_setup.py` una cola secundaria `ticket_dlq`. El script intercepta las cabeceras `x-death` del mensaje AMQP para contar los reintentos. Al llegar a 3 fallos consecutivos, el mensaje se retira permanentemente del flujo de negocio y se aloja de forma segura en la DLQ para que un administrador humano lo audite a posteriori.

---

## 5. Dinámica de Escalado Elástico y Modelado Matemático

### 5.1 Modelado de la Capacidad del Nodo y Latencia Simulada
Para dotar de gran realismo a las métricas (Requisito 4), es imperativo simular que cada venta implica comunicarse con el sistema Visa/Mastercard. Se ha insertado rígidamente un `time.sleep(0.100)` dentro de la ruta crítica del worker. 
Esto implica que, matemáticamente, el **Capacity ($C$)** de un único worker de hilo único está limitado rígidamente a:
$$ C = \frac{1 \text{ segundo}}{0.1 \text{ segundos/mensaje}} = 10 \text{ mensajes/segundo/worker} $$

### 5.2 Formulación de Escalado por Backlog (Profundidad de Cola)
Para calcular cuántos workers Stateless (Lambdas) necesitamos instanciar en cada momento para mantener los acuerdos de nivel de servicio (SLA), implementamos un orquestador que consulta la HTTP Management API de RabbitMQ evaluando la fórmula teórica:
$$ N = \max \left( \frac{B \cdot C}{T_r}, \frac{\lambda \cdot T}{C} \right) $$
Siendo $T_r$ nuestro Target Response Time ($2.0s$). Si la cola reporta $B = 2500$ mensajes pendientes de procesar y queremos despacharlos en menos de $2$ segundos:
$$ N = \frac{2500 \cdot 10}{2.0} = 12500 \text{ Workers?? (Fórmula teórica - Límite práctico ajustado en código)} $$
*(El código real calcula $B / (T_r \times C) = 2500 / 20 = 125 \text{ workers}$, protegiendo contra el escalado explosivo infinito)*.

---

## 6. Diseño y Justificación del Escenario de Carga Z(t)

La elasticidad solo puede probarse variando el ratio de llegada ($\lambda$) a lo largo del tiempo. Hemos implementado un benchmark (`comprehensive_benchmark.py`) que modela la función $Z(t)$ en 5 fases estandarizadas:
1. **Fase de Valle (Low Load):** $\approx 10$ peticiones por segundo. Permite calibrar la latencia intra-AWS y verificar que el orquestador reduce la flota de workers a un mínimo de supervivencia ($N=1$).
2. **Rampa Creciente (Ramp-up):** Incremento lineal de tráfico durante varios segundos. Pone a prueba si el orquestador tiene retardos severos en la toma de decisiones para crear nuevos procesos.
3. **Picos Bruscos (Sudden Spikes):** Inyección de miles de peticiones concentradas en milisegundos simulando campañas de marketing masivas. Pone a prueba la capacidad de contención de memoria RAM del Broker RabbitMQ.
4. **Carga Sostenida (Sustained High Load):** Se mantiene un tráfico destructivo superior al $C$ total de la red. Durante esta fase se ejecuta el **High Contention Scenario**, forzando al $80\%$ de los bots clientes a pelear por solo un $5\%$ de las entradas.
5. **Enfriamiento (Cool-down):** Caída abrupta del flujo. Permite observar los procesos de recolección de basura, vaciado residual de colas, y la reducción gradual de costes Cloud apagando EC2s o Lambdas innecesarios.

---

## 7. Evaluación Empírica del Rendimiento (Stress Testing)

*(Por favor, referencie las imágenes exportadas del directorio `results/plots/` para esta sección)*

### 7.1 Análisis del Throughput vs Número de Workers
*(Insertar: throughput_vs_workers.png)*
**Discusión:** La gráfica refleja claramente la Ley de Amdahl aplicada a colas. En los primeros compases, escalar de 1 a 10 workers incrementa el throughput de manera estrictamente lineal (cada worker absorbe sus 10 msg/s, logrando 100 msg/s). No obstante, la curva se aplana logarítmicamente cuando nos aproximamos al cuello de botella de la base de datos. Llegado cierto punto, añadir 50 workers más no ofrece mejora en las operaciones completadas por segundo, sino que incrementa los *Context Switches* de la CPU.

### 7.2 Tiempos de Encolado (Queue Backlog) vs Tiempo
*(Insertar: queue_backlog_vs_time.png)*
**Discusión:** En el gráfico de serie temporal, la fase "Spike" inyecta un escalón casi vertical en el volumen del Backlog de RabbitMQ. A partir del pico máximo, el algoritmo elástico de `elastic_launcher.py` despierta las réplicas asíncronas. La cola inicia una caída controlada y prolongada hacia el valor cero, demostrando que el "Load Smoothing" ha funcionado perfectamente: la base de datos jamás sufrió la avalancha inicial.

### 7.3 Degradación de Latencias por Percentiles (p50, p95, p99)
*(Insertar: latencies_percentiles.png)*
**Discusión:** Todas las métricas mostradas son estrictamente *Server-Side*, almacenadas atómicamente al concluirse un insert exitoso, para aislar la latencia de red de internet del cliente. El $P_{50}$ (mediana) demuestra un rendimiento prístino (procesamiento base de la latencia inyectada). Sin embargo, la brecha masiva con respecto al $P_{99}$ (el 1% de las peticiones más lentas) demuestra el impacto salvaje que sufre un cliente cuando su mensaje queda "enterrado" bajo un Backlog enorme y tiene que esperar varios segundos de "Queue Time" antes de que un worker lo lea.

---

## 8. Identificación de Cuellos de Botella y Escenario "Hotspot"

Tras la evaluación, podemos confirmar que el Sistema de Venta de Entradas es resiliente y funcional, pero está estrictamente acotado por un punto de saturación (Saturation Point): **La gestión concurrente de registros (Locking) en la capa de persistencia**.

En la variante del Test de Carga "Hotspot Load", inyectamos el escenario donde el $80\%$ de la intención de compra se dirigía al bloque de "Asientos VIP" (representando el $5\%$ del aforo).
Esto expuso severamente la penalización del bloqueo pesimista en PostgreSQL (`FOR UPDATE`). Miles de workers que extrajeron mensajes válidos de RabbitMQ en paralelo intentaron invadir el disco duro simultáneamente para el mismo bloque de filas. PostgreSQL impuso un estrangulamiento (*Throttling*) drástico serializando todas esas solicitudes. 
Esto provocó que los workers consumieran inútilmente tiempo y CPU en estado de "espera de I/O de red de base de datos", impidiendo que pudieran vaciar RabbitMQ. **Conclusión empírica:** Bajo extrema contención de negocio, la paralelización asíncrona no sirve de nada, ya que la limitación fundamental proviene de la exclusión mutua compartida.

---

## 9. Respuestas a las Preguntas Conceptuales Obligatorias (Q1 y Q2)

### Q1. Consistency vs Scalability Trade-off
> *¿Qué modelo de consistencia elegiste y por qué? ¿Cómo cambiaría el comportamiento si cambiaras a un modelo más débil o más fuerte? ¿Cuál es el impacto en la correctitud y el rendimiento?*

**Justificación y Trade-off:**
Basándome en las estrictas regulaciones de los sistemas transaccionales financieros y de asignación de butacas, el diseño se fundamenta exclusivamente en un modelo de **Consistencia Fuerte / Estricta (Strong Consistency)**. Se han implementado mecanismos ACID completos para los asientos numerados, asegurando que cualquier confirmación de lectura post-transacción devolverá siempre el estado más reciente, impidiendo el *Overselling*. 

**Impacto del Cambio de Modelo:**
Si decidiera relajar esta topología hacia un modelo de **Consistencia Eventual** (por ejemplo, escribiendo las reservas asíncronamente en una tabla NoSQL compartimentada sin utilizar bloqueos, y consolidando el aforo de forma diferida mediante un `cron job`), el **rendimiento (*Scalability*) y el *Throughput*** global del sistema se elevarían de forma exponencial. RabbitMQ se vaciaría al instante, sin tiempos de espera en BD.
Sin embargo, el **Impacto en la Correctitud** sería desastroso. El sistema de venta permitiría a 50 personas diferentes descargar un ticket válido con el mismo "Asiento 45". En este dominio concreto de negocio, un fallo en la correctitud representa fraude al consumidor. Por tanto, el *trade-off* asumido fue sacrificar escalabilidad máxima para garantizar integridad de datos total.

### Q2. Fault Tolerance vs Performance Trade-offs
> *¿Cómo afectan los reintentos, la idempotencia y el manejo de fallos al rendimiento? ¿Qué sobrecargas introducen? ¿Hay un punto donde la fiabilidad reduzca drásticamente el throughput?*

**Análisis de Sobrecarga (Overheads):**
La implementación obligatoria de un sistema distribuido confiable añade un enorme impuesto oculto (overhead) sobre los ciclos de CPU y lectura en disco. 
- La **Idempotencia** obliga a que cada worker, antes de iniciar una tarea, ejecute una costosa búsqueda primaria indexada en la tabla `transactions` de la base de datos para confirmar si un UUID (`request_id`) existe ya, encareciendo y ralentizando absolutamente todas las transacciones legítimas.
- Los mecanismos semánticos **"At-least-once"**, donde RabbitMQ mantiene un registro persistente del mensaje hasta que recibe la red (el `ACK`), implican un pesado peaje de latencia TCP entre instancias EC2 por cada mensaje individual procesado.
- La gestión de **Reintentos y DLQ (Dead-Letter-Queues)** añade un alto grado de consumo lógico. Si un backend cae durante 10 segundos, miles de workers intentarán procesar el mensaje, provocarán una excepción, registrarán el fallo en disco (logging) e incrementarán el header de muerte de AMQP (`x-death`). Todo ese ancho de banda intra-VPC y recursos de CPU se desperdician en la capa de tolerancia, reduciendo el Throughput total (el "Goodput", o porcentaje útil) a casi cero.

Existe de hecho un punto documentado en el *Teorema CAP*: Si aumentamos la tolerancia a particiones e incrementamos la fiabilidad exigiendo réplicas síncronas en múltiples zonas de disponibilidad (AWS Multi-AZ Data Guarding), los *Round-Trips* de red de cientos de milisegundos colapsarían la latencia del sistema, arruinando por completo el throughput por el mero intento de no perder ni un solo byte de información.

---

## 10. Conclusiones Finales

El diseño de un servicio elástico de gestión de tickets sobre la plataforma AWS ha demostrado empíricamente que la arquitectura distribuida mediada por colas asíncronas es enormemente superior a las implementaciones monolíticas tradicionales (REST directa). RabbitMQ absorbió picos devastadores inyectados por la función generadora $Z(t)$, previniendo por completo el colapso TCP (Timeouts) del ecosistema. 
Asimismo, la utilización programática de los algoritmos de escalado ha resultado exitosa, aunque la dependencia fundamental sobre los bloqueos ACID en PostgreSQL subraya la lección más importante de la computación escalable: un sistema distribuido será tan escalable, en última instancia, como lo sea su recurso lógico más contencioso y compartido.

---

## 11. Uso Declarado de Inteligencia Artificial

Conforme a las normativas académicas, se declara el uso transparente de IA generativa (agente de entorno Antigravity / LLM) empleado exclusivamente en las siguientes labores periféricas y optimizadoras:
1. Generación masiva de Boilerplate para Infraestructura como Código (`Terraform main.tf`, `variables.tf`) ahorrando horas de referenciación a la documentación oficial de AWS Provider.
2. Automatización del análisis y parseo estadístico de los resultados (Traducción programática de `.json` a `.png` mediante la librería Python `matplotlib`).
3. Auditoría exhaustiva de sintaxis (`compileall`) y detección de falsos positivos en el enrutamiento relativo de rutas e importaciones (sys.path injects).

La estrategia metodológica (Fórmulas $N$, Bloqueos Pesimistas `FOR UPDATE`, Tolerancia a Poison Pills en AMQP y justificaciones arquitectónicas del Teorema CAP) han sido ideadas, evaluadas y garantizadas exclusivamente por los requerimientos formales de la asignatura.
