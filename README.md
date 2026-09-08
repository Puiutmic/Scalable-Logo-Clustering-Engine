# Scalable Logo Clustering Engine

This is a personal project I built to practice building robust data pipelines and asynchronous network engines. The goal of this system is to ingest a large list of domains, extract their brand logos from the web, and group them by visual similarity without relying on heavy Machine Learning models.

## Architecture Overview

Instead of using standard ML clustering algorithms (like K-Means or DBSCAN), I wanted to build a fast, deterministic system. The core logic relies on Perceptual Hashing combined with a Disjoint Set Union (DSU) data structure.

Here is the data flow:

+-------------+       +------------------+       +------------------+       +------------------+
| Dataset     |       | Async Scraper    |       | Image Processor  |       | DSU Clusterer    |
| (.parquet)  | ----> | (aiohttp / bs4)  | ----> | (pHash / RAM)    | ----> | (Graph Logic)    |
+-------------+       +------------------+       +------------------+       +------------------+
                             |                           |                           |
                     Scans metadata,             Converts image to           Groups hashes with
                     icons, and tags             64-bit fingerprint          Hamming Distance <= 8

## Key Technical Decisions

1. Asynchronous I/O: 
Web scraping is I/O bound. I used `aiohttp` and `asyncio` to process hundreds of requests concurrently, drastically reducing execution time compared to synchronous loops.

2. Fallback Extraction Logic:
Websites store logos in different places. The scraper checks `og:image` metadata first, falls back to `rel="icon"`, and finally parses standard `<img>` tags looking for specific class names or alt texts.

3. Perceptual Hashing (pHash):
Unlike cryptographic hashes (MD5, SHA), a perceptual hash represents the visual frequencies of an image. If two logos are identical but have different resolutions, their pHash will remain nearly the same.

4. Disjoint Set Union (DSU):
To group similar logos, I calculate the Hamming distance between their hashes. If the distance is under a certain threshold, the DSU algorithm instantly connects them in a graph, forming components (groups). This is highly efficient for memory and CPU.

5. In-Memory Processing:
To prevent disk bottlenecking, downloaded images are converted to bytes (`io.BytesIO`) and processed directly in RAM.

## How to Run

The easiest way to run the engine is via Docker, ensuring all networking and OS-level dependencies are handled automatically.

1. Clone the repository.
2. Place your target dataset (e.g., a `.parquet` file with a `domain` column) in the root directory.
3. Run the following command:

docker-compose up --build

The system will output its progress and generate a `results.json` file containing the clustered domains.