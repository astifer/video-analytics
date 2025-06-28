# tests/test_pipeline.py
import pytest
import aiohttp
import asyncio

from shared.utils import settings
API = settings.public_urls.get("API_URL")


@pytest.mark.asyncio
async def test_pipeline_flow():
    await asyncio.sleep(20)
    async with aiohttp.ClientSession() as session:
        # 1. Создать сценарий
        await asyncio.sleep(3)
        create_url = f"http://{API}/scenario/"

        async with session.post(create_url) as response:
            assert response.status == 200
            data = await response.json()
            scenario_id = data["scenario_id"]


        # 2. Запустить обработку
        await asyncio.sleep(3)
        headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
        }
        json_data = {
            "new_status": "in_startup_processing"
        }
        update_url = f"http://{API}/scenario/{scenario_id}/"

        async with aiohttp.ClientSession() as session:
            async with session.post(update_url, headers=headers, json=json_data) as response:
                assert response.status == 200

                print("Response:", await response.text())


        # bad request
        await asyncio.sleep(3)
        json_data['new_status'] = "inactive"

        async with aiohttp.ClientSession() as session:
            async with session.post(update_url, headers=headers, json=json_data) as response:
                assert response.status == 200
                data = await response.json()
                data = data['details']

                assert ['details'] in data
                assert ['scenario_id'] not in data
                print("Response:", await response.text())


        # 3. Запустить сценарий
        await asyncio.sleep(3)
        headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
        }
        json_data = {
            "new_status": "active"
        }
        update_url = f"http://{API}/scenario/{scenario_id}/"

        async with aiohttp.ClientSession() as session:
            async with session.post(update_url, headers=headers, json=json_data) as response:
                assert response.status == 200
                data = await response.json()
                data = data['details']

                assert ['scenario_id'] in data
                assert ['status'] == 'active'

                print("Response:", await response.text())

        # 4. Получить предсказания
        await asyncio.sleep(10)

        headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
        }
        json_data = {
            "new_status": "active"
        }
        update_url = f"http://{API}/scenario/{scenario_id}/"

        async with aiohttp.ClientSession() as session:
            async with session.get(update_url, headers=headers, json=json_data) as response:
                assert response.status == 200
                data = await response.json()
                data = data['details']

                assert ['scenario_id'] in data
                assert ['status'] == 'active'
                assert 'prediction' in data['details']['payload']

                print("Response:", await response.text())