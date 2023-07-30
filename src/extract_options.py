from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd
import json
from bs4.element import Tag
from pathlib import Path


def _parse_multiselect(soup: Tag) -> pd.DataFrame:
    mselect_item = soup.select_one(".multiselect .multiselect-input")
    mselect_opts = mselect_item.get("data-categories-options")
    return pd.DataFrame(
        [
            {
                "category": cat["category"],
                "category_id": cat["categoryId"],
                "subcategory": subcat,
                "subcategory_id": subcat_id,
            }
            for cat in json.loads(mselect_opts)
            for (subcat_id, subcat) in cat["options"].items()
        ]
    )


def _parse_select(select_elem: Tag) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"name": elem.get_text(), "id": elem.get("value")}
            for elem in select_elem.select("option")
        ]
    )


if __name__ == "__main__":
    OPTIONS_DIR = Path("options")
    OPTIONS_DIR.mkdir(exist_ok=True)

    # PARSE CONTENT PAGE
    soup = BeautifulSoup(Path("page/content.html").read_bytes(), "html5lib")
    sector_df = _parse_multiselect(soup=soup.select_one("#sectors")).set_index(
        ["category_id", "subcategory_id"]
    )
    sector_df.to_parquet(OPTIONS_DIR / "sector.parquet")

    # PARSE DATA PAGE
    soup = BeautifulSoup(Path("page/data.html").read_bytes(), "html5lib")

    position_df = (
        _parse_multiselect(soup=soup.select_one("#classification_type_swa"))
        .drop(columns=["category", "category_id"])
        .set_index(["subcategory_id"])
    )
    position_df.to_parquet(OPTIONS_DIR / "position.parquet")

    work_schedule_df = _parse_select(soup.select_one("#work_schedule")).set_index("id")
    work_schedule_df.to_parquet(OPTIONS_DIR / "work_schedule.parquet")

    qualification_df = _parse_select(soup.select_one("#qualification")).set_index("id")
    qualification_df.to_parquet(OPTIONS_DIR / "qualification.parquet")

    experience_df = _parse_select(soup.select_one("#experience")).set_index("id")
    experience_df.to_parquet(OPTIONS_DIR / "experience.parquet")

    drive_license_df = _parse_select(soup.select_one("#drive_license_id")).set_index(
        "id"
    )
    drive_license_df.to_parquet(OPTIONS_DIR / "drive_license.parquet")

    language_df = _parse_select(
        soup.find("select", attrs={"id": "lang[0][lang_id]"})
    ).set_index("id")
    language_df.to_parquet(OPTIONS_DIR / "language.parquet")

    language_level_df = _parse_select(
        soup.find("select", attrs={"id": "lang[0][lang_level]"})
    ).set_index("id")
    language_level_df.to_parquet(OPTIONS_DIR / "language_level.parquet")

    salary_bonus_df = (
        _parse_multiselect(soup=soup.find("div", attrs={"data-id": "salary_bonus"}))
        .drop(columns=["category", "category_id"])
        .set_index(["subcategory_id"])
    )
    salary_bonus_df.to_parquet(OPTIONS_DIR / "salary_bonus.parquet")

    equipment_df = (
        _parse_multiselect(soup=soup.find("div", attrs={"data-id": "equipment"}))
        .drop(columns=["category", "category_id"])
        .set_index(["subcategory_id"])
    )
    equipment_df.to_parquet(OPTIONS_DIR / "equipment.parquet")

    extra_df = (
        _parse_multiselect(soup=soup.find("div", attrs={"data-id": "workplaceExtras"}))
        .drop(columns=["category", "category_id"])
        .set_index(["subcategory_id"])
    )
    extra_df.to_parquet(OPTIONS_DIR / "extra.parquet")
