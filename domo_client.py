# import requests

# class DomoClient:
#     def __init__(self, base_url, headers):
#         self.base_url = base_url.rstrip("/")
#         self.headers = headers

#     def create_card(self, page_id, payload):
#         url = f"{self.base_url}/api/content/v3/cards/kpi?pageId={page_id}"

#         print("CREATE CARD")
#         print("METHOD: PUT")
#         print("URL:", url)

#         resp = requests.put(   
#             url,
#             headers=self.headers,
#             json=payload
#         )

#         print("STATUS:", resp.status_code)
#         # print("RESPONSE TEXT:", resp.text)

#         if resp.status_code not in (200, 201):
#             raise RuntimeError(
#                 f"Domo API error {resp.status_code}: {resp.text}"
#             )

#         return resp.json()
# ---------------------

import requests
import json

class DomoClient:
    def __init__(self, base_url, headers):
        self.base_url = base_url.rstrip("/")
        self.headers = headers

    def create_card(self, page_id, payload):
        url = f"{self.base_url}/api/content/v3/cards/kpi?pageId={page_id}"

        print("\n" + "="*60)
        print("CREATE CARD")
        print("METHOD: PUT")
        print("URL:", url)
        print("TITLE:", payload.get("definition", {}).get("title", "unknown"))

        # Print beast mode formulas from dsUpdated (correct location)
        formulas_section = payload.get("definition", {}).get("formulas", {})
        formulas = formulas_section.get("dsUpdated", [])
        if formulas:
            print(f"BEAST MODE FORMULAS ({len(formulas)}):")
            for f in formulas:
                print(f"  id={f.get('id')}  name={f.get('name')}  formula={f.get('formula')}")
        else:
            print("BEAST MODE FORMULAS: none")

        # Print subscription columns
        subs = payload.get("definition", {}).get("subscriptions", {})
        for sub_name, sub in subs.items():
            cols = sub.get("columns", [])
            print(f"SUBSCRIPTION [{sub_name}] columns:")
            for c in cols:
                print(f"  {c}")

        resp = requests.put(
            url,
            headers=self.headers,
            json=payload
        )

        print("STATUS:", resp.status_code)

        if resp.status_code not in (200, 201):
            print("ERROR RESPONSE:", resp.text)
            raise RuntimeError(
                f"Domo API error {resp.status_code}: {resp.text}"
            )

        print("SUCCESS ✅")
        print("="*60)
        return resp.json()
