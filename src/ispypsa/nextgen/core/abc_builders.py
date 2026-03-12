from abc import ABC, abstractmethod
import pypsa


class ZonalHubBuilder(ABC):
    """
    Abstract Base Class for mapping complex spatial regions into simplified 'Zonal Hubs'.
    """

    @abstractmethod
    def build_hub(self, network: pypsa.Network, region_name: str) -> None:
        """
        Constructs multi-carrier buses, generators, and loads for a given zonal hub.
        """
        pass


class SectorLinkBuilder(ABC):
    """
    Abstract Base Class for building inter-carrier and inter-hub links.
    """

    @abstractmethod
    def build_links(self, network: pypsa.Network) -> None:
        """
        Construct standard transport/transmission links between Zonal Hubs.
        """
        pass
