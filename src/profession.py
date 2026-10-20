from io import BytesIO
from typing import Union

import aswan
import datazimmer as dz
import pandas as pd

index_url = "https://www.profession.hu/sitemap-listings-index-hu.xml"


class AdHandler(aswan.RequestHandler):
    max_in_parallel = 2
    url_root = "https://www.profession.hu"

    def parse(self, blob):
        """Byte Local Object"""
        return blob

    def is_session_broken(self, result: Union[int, Exception]):
        return True


class SitemapHandler(aswan.RequestHandler):
    max_in_parallel = 2

    def parse(self, blob):
        listing_df = (
            pd.read_xml(BytesIO(blob))
            .assign(
                ad_id=lambda _df: _df["loc"].str.split("-").str[-1].astype(int),
                lastmod=lambda _df: pd.to_datetime(_df["lastmod"], utc=True),
            )
            .drop(columns=["changefreq"], errors="ignore")
            .rename(columns={"loc": "url"})
        )
        self.register_links_to_handler(
            links=listing_df["url"].values,
            handler_cls=AdHandler,
        )
        return listing_df

    def is_session_broken(self, result: Union[int, Exception]):
        return True


class SitemapIndexHandler(aswan.RequestHandler):
    def parse(self, blob):
        index_df = pd.read_xml(BytesIO(blob))
        self.register_links_to_handler(
            links=index_df["loc"].values,
            handler_cls=SitemapHandler,
            overwrite=True,
        )
        return index_df

    def is_session_broken(self, result: Union[int, Exception]):
        return True


class OfferDzA(dz.DzAswan):
    name: str = "profession"
    cadence: str = "1d"
    starters = {SitemapIndexHandler: [index_url], AdHandler: []}
