from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import os
import asyncio
import aiohttp
import logging
from dotenv import load_dotenv
import psutil
import fitz
import base64

app = FastAPI()

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))  # Use the PORT environment variable
    uvicorn.run("main:app", host="0.0.0.0", port=port)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)


class PDFUrl(BaseModel):
    url: HttpUrl


def log_resource_usage(stage):
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    cpu_percent = process.cpu_percent(interval=None)
    logging.info(f"{stage} - Memory Usage: {mem_info.rss / (1024 * 1024):.2f} MB, CPU Usage: {cpu_percent}%")


async def download_pdf(url: str) -> bytes:
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, allow_redirects=True) as response:
                logging.info(f"Response status: {response.status}")
                logging.info(f"Response headers: {response.headers}")
                if response.status != 200:
                    raise HTTPException(status_code=400,
                                        detail=f"Failed to download PDF. Status code: {response.status}")
                content_type = response.headers.get('Content-Type', '')
                logging.info(f"Content-Type: {content_type}")
                if 'pdf' not in content_type.lower():
                    raise HTTPException(status_code=400,
                                        detail=f"URL does not point to a PDF file. Content-Type: {content_type}")
                pdf_bytes = await response.read()
                MAX_PDF_SIZE = 100 * 1024 * 1024  # 100 MB
                if len(pdf_bytes) > MAX_PDF_SIZE:
                    raise HTTPException(status_code=400, detail="PDF file is too large.")
                return pdf_bytes
    except aiohttp.ClientError as e:
        logging.error(f"Client error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail="Client error occurred while downloading PDF.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error occurred.")


async def convert_pdf_to_images(pdf_bytes: bytes):
    try:
        log_resource_usage("Before Conversion")
        image_bytes_list = []
        MAX_PAGE_COUNT = 5000  # Limit the number of pages to process
        DPI = 100  # Set DPI to reduce resource usage
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            page_count = doc.page_count
            logging.info(f"PDF has {page_count} pages.")
            if page_count > MAX_PAGE_COUNT:
                raise HTTPException(
                    status_code=400,
                    detail=f"PDF has too many pages ({page_count}). Maximum allowed is {MAX_PAGE_COUNT}."
                )
            for page_num in range(min(page_count, MAX_PAGE_COUNT)):
                page = doc.load_page(page_num)
                pix = page.get_pixmap(dpi=DPI)
                image_bytes = pix.tobytes("png")
                image_bytes_list.append((page_num + 1, image_bytes))  # Include page number
        log_resource_usage("After Conversion")
        return image_bytes_list
    except Exception as e:
        logging.error(f"Error converting PDF to images: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error converting PDF to images.")


@app.post("/convert-pdf")
async def convert_pdf(pdf: PDFUrl):
    pdf_bytes = await download_pdf(str(pdf.url))
    image_bytes_list = await convert_pdf_to_images(pdf_bytes)

    # Base64 encode images with labels
    base64_images = []
    for page_num, image_bytes in image_bytes_list:
        encoded_image = base64.b64encode(image_bytes).decode('utf-8')
        base64_images.append({
            "page": page_num,
            "image_data": encoded_image
        })

    if not base64_images:
        raise HTTPException(status_code=500, detail="No images were generated.")

    return {"images": base64_images}


@app.get("/health")
async def health():
    return {"status": "ok"}

