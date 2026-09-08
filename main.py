import os
import io
import json
import asyncio
from time import perf_counter
from urllib.parse import urljoin, urlparse
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from PIL import Image, UnidentifiedImageError
import imagehash


# ============================================================
# CONFIGURATION
# ============================================================

DATASET_PATH = "logos.snappy.parquet"
RESULTS_PATH = "results.json"
HASHES_PATH = "hashes.tsv"

DOMAIN_LIMIT = 300
MAX_CONCURRENCY = 40
HAMMING_THRESHOLD = 8

REQUEST_TIMEOUT_SECONDS = 15
MAX_IMAGE_BYTES = 8 * 1024 * 1024  # 8 MB

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.5",
}


# ============================================================
# DISJOINT SET UNION
# Path compression + union by size
# ============================================================

class DisjointSetUnion:
    def __init__(self, items):
        self.parent = {item: item for item in items}
        self.size = {item: 1 for item in items}

    def find(self, item):
        """
        Find the representative/root of a set.

        Path compression flattens the tree during lookup,
        improving the cost of future operations.
        """
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])

        return self.parent[item]

    def union(self, a, b):
        """
        Merge the sets containing a and b.

        Uses union by size so that the smaller tree is attached
        underneath the larger one.
        """
        root_a = self.find(a)
        root_b = self.find(b)

        if root_a == root_b:
            return False

        if self.size[root_a] < self.size[root_b]:
            root_a, root_b = root_b, root_a

        self.parent[root_b] = root_a
        self.size[root_a] += self.size[root_b]

        return True


# ============================================================
# HTML / LOGO EXTRACTION
# ============================================================

def normalize_rel(rel_value):
    """
    BeautifulSoup may expose rel as either a string or a list.
    Convert both forms into lowercase text.
    """
    if isinstance(rel_value, list):
        return " ".join(rel_value).lower()

    return str(rel_value or "").lower()


async def get_logo_url(session, semaphore, domain):
    """
    Fetch a website and attempt to locate its primary visual asset.

    Extraction order:
        1. OpenGraph metadata
        2. icon / apple-touch-icon links
        3. heuristic <img> search
    """
    url = f"https://{domain}"

    try:
        async with semaphore:
            async with session.get(
                url,
                allow_redirects=True
            ) as response:

                if response.status != 200:
                    return None

                html = await response.text(errors="ignore")

        soup = BeautifulSoup(html, "html.parser")

        # ----------------------------------------------------
        # Layer 1: OpenGraph image
        # ----------------------------------------------------

        meta = soup.find("meta", property="og:image")

        if meta and meta.get("content"):
            return urljoin(str(response.url), meta["content"])

        # ----------------------------------------------------
        # Layer 2: icons
        # ----------------------------------------------------

        for link in soup.find_all("link", href=True):
            rel = normalize_rel(link.get("rel"))

            if any(
                keyword in rel
                for keyword in (
                    "icon",
                    "apple-touch-icon",
                    "logo",
                )
            ):
                return urljoin(
                    str(response.url),
                    link["href"]
                )

        # ----------------------------------------------------
        # Layer 3: heuristic image search
        # ----------------------------------------------------

        keywords = (
            "logo",
            "brand",
            "nav-img",
            "navbar",
        )

        for img in soup.find_all("img"):
            src = (
                img.get("src")
                or img.get("data-src")
                or img.get("data-lazy-src")
                or ""
            )

            if not src:
                continue

            alt = str(img.get("alt", "")).lower()
            css_class = " ".join(img.get("class", [])).lower()
            element_id = str(img.get("id", "")).lower()
            src_lower = src.lower()

            if any(
                keyword in src_lower
                or keyword in alt
                or keyword in css_class
                or keyword in element_id
                for keyword in keywords
            ):
                return urljoin(
                    str(response.url),
                    src
                )

    except (
        aiohttp.ClientError,
        asyncio.TimeoutError,
        UnicodeDecodeError,
    ):
        return None

    return None


# ============================================================
# IMAGE PROCESSING
# ============================================================

def compute_phash(image_bytes):
    """
    CPU-bound image decoding and perceptual hashing.

    This function is executed through asyncio.to_thread()
    so image processing does not block the event loop.
    """
    with Image.open(io.BytesIO(image_bytes)) as image:
        image = image.convert("RGB")
        return imagehash.phash(image)


async def download_logo(session, semaphore, logo_url):
    """
    Download an image while keeping HTTP concurrency bounded.
    """
    parsed_path = urlparse(logo_url).path.lower()

    # Pillow does not natively decode SVG files.
    if parsed_path.endswith(".svg"):
        return None

    try:
        async with semaphore:
            async with session.get(
                logo_url,
                allow_redirects=True
            ) as response:

                if response.status != 200:
                    return None

                content_type = (
                    response.headers
                    .get("Content-Type", "")
                    .lower()
                )

                # Skip responses that clearly are not images.
                if (
                    content_type
                    and not content_type.startswith("image/")
                ):
                    return None

                if "svg" in content_type:
                    return None

                content_length = response.headers.get(
                    "Content-Length"
                )

                if content_length:
                    try:
                        if int(content_length) > MAX_IMAGE_BYTES:
                            return None
                    except ValueError:
                        pass

                image_bytes = await response.read()

                if len(image_bytes) > MAX_IMAGE_BYTES:
                    return None

                return image_bytes

    except (
        aiohttp.ClientError,
        asyncio.TimeoutError,
    ):
        return None


async def process_domain(
    session,
    semaphore,
    domain
):
    """
    Complete processing pipeline for one domain:

        HTML fetch
            ->
        logo discovery
            ->
        image download
            ->
        perceptual hash

    Multiple domains execute concurrently.
    """
    logo_url = await get_logo_url(
        session,
        semaphore,
        domain
    )

    if not logo_url:
        return None

    image_bytes = await download_logo(
        session,
        semaphore,
        logo_url
    )

    if not image_bytes:
        return None

    try:
        logo_hash = await asyncio.to_thread(
            compute_phash,
            image_bytes
        )

        return domain, logo_hash

    except (
        UnidentifiedImageError,
        OSError,
        ValueError,
    ):
        return None


# ============================================================
# OUTPUT
# ============================================================

def save_hashes(hashes):
    """
    Export the 64-bit perceptual hashes.

    """
    with open(
        HASHES_PATH,
        "w",
        encoding="utf-8"
    ) as file:

        for domain, logo_hash in hashes.items():
            file.write(
                f"{domain}\t{str(logo_hash)}\n"
            )


def save_clusters(groups):
    with open(
        RESULTS_PATH,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            list(groups.values()),
            file,
            indent=4
        )


# ============================================================
# PYTHON CLUSTERING BASELINE
# ============================================================

def cluster_hashes(hashes):
    """
    Current Python O(N^2) clustering implementation.

    Similarity detection:
        Pairwise Hamming-distance comparisons.

    Cluster construction:
        DSU with path compression and union by size.

    We keep this implementation as the baseline that will later
    be benchmarked against the C++20 clustering engine.
    """
    items = list(hashes.keys())

    dsu = DisjointSetUnion(items)

    comparisons = 0
    unions = 0

    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            comparisons += 1

            domain_a = items[i]
            domain_b = items[j]

            distance = (
                hashes[domain_a]
                - hashes[domain_b]
            )

            if distance <= HAMMING_THRESHOLD:
                if dsu.union(domain_a, domain_b):
                    unions += 1

    groups = {}

    for domain in items:
        root = dsu.find(domain)

        groups.setdefault(
            root,
            []
        ).append(domain)

    return groups, comparisons, unions


# ============================================================
# MAIN PIPELINE
# ============================================================

async def main():
    total_start = perf_counter()

    print("=" * 60)
    print("SCALABLE LOGO CLUSTERING ENGINE")
    print("=" * 60)

    # --------------------------------------------------------
    # Dataset loading
    # --------------------------------------------------------

    print("\n[1/4] Reading dataset...")

    dataset_start = perf_counter()

    df = pd.read_parquet(
        DATASET_PATH,
        engine="pyarrow"
    )

    domains = (
        df["domain"]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .tolist()
    )

    subset = domains[:DOMAIN_LIMIT]

    dataset_time = (
        perf_counter()
        - dataset_start
    )

    if not subset:
        raise RuntimeError(
            "No valid domains were found in the dataset."
        )

    print(
        f"Loaded {len(domains):,} unique domains."
    )

    print(
        f"Processing first {len(subset):,} domains."
    )

    # --------------------------------------------------------
    # Async extraction + hashing
    # --------------------------------------------------------

    print(
        "\n[2/4] Starting asynchronous "
        "logo extraction..."
    )

    extraction_start = perf_counter()

    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT_SECONDS,
        connect=5
    )

    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENCY
    )

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENCY
    )

    hashes = {}

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=HEADERS,
    ) as session:

        tasks = [
            asyncio.create_task(
                process_domain(
                    session,
                    semaphore,
                    domain
                )
            )
            for domain in subset
        ]

        for future in asyncio.as_completed(tasks):
            result = await future

            if result is None:
                continue

            domain, logo_hash = result
            hashes[domain] = logo_hash

            print(
                f"[SUCCESS] "
                f"{domain:<40} "
                f"{logo_hash}"
            )

    extraction_time = (
        perf_counter()
        - extraction_start
    )

    success_rate = (
        len(hashes)
        / len(subset)
        * 100
    )

    # --------------------------------------------------------
    # Export hashes
    # --------------------------------------------------------

    save_hashes(hashes)

    # --------------------------------------------------------
    # Python DSU clustering
    # --------------------------------------------------------

    print(
        "\n[3/4] Running Python "
        "DSU clustering baseline..."
    )

    clustering_start = perf_counter()

    groups, comparisons, unions = (
        cluster_hashes(hashes)
    )

    clustering_time = (
        perf_counter()
        - clustering_start
    )

    save_clusters(groups)

    # --------------------------------------------------------
    # Metrics
    # --------------------------------------------------------

    total_time = (
        perf_counter()
        - total_start
    )

    print(
        "\n[4/4] Complete."
    )

    print("\n" + "=" * 60)
    print("PERFORMANCE METRICS")
    print("=" * 60)

    print(
        f"Domains attempted:       "
        f"{len(subset):,}"
    )

    print(
        f"Logos hashed:            "
        f"{len(hashes):,}"
    )

    print(
        f"Extraction success rate: "
        f"{success_rate:.2f}%"
    )

    print(
        f"Clusters generated:      "
        f"{len(groups):,}"
    )

    print(
        f"Pairwise comparisons:    "
        f"{comparisons:,}"
    )

    print(
        f"Successful DSU unions:   "
        f"{unions:,}"
    )

    print()

    print(
        f"Dataset load time:       "
        f"{dataset_time:.3f}s"
    )

    print(
        f"Extraction + hashing:    "
        f"{extraction_time:.3f}s"
    )

    print(
        f"Python clustering:       "
        f"{clustering_time:.6f}s"
    )

    print(
        f"Total execution time:    "
        f"{total_time:.3f}s"
    )

    print()

    print(
        f"Hashes written to:       "
        f"{HASHES_PATH}"
    )

    print(
        f"Clusters written to:     "
        f"{RESULTS_PATH}"
    )

    print("=" * 60)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    if os.name == "nt":
        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy()
        )

    asyncio.run(main())