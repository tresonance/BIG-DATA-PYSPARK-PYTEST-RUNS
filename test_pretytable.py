# --------------------------------------------------------- #
# test_prettytable.py
#
# [DESCRIPION]: Simply display data inside PrettyTable
#
# ---------------------------------------------------------- #
import os, sys 

# Get the path to the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to IT_DEPENDENCIES
dependencies_path = os.path.abspath(os.path.join(
    script_dir,
    '../shared-whiteboard-client/dist/shared-whiteboard-client/browser/TOUS_MES_COURS/PYTEST_DIR/IT_DEPENDENCIES'
))

# Add this path to sys.path
sys.path.append(dependencies_path)

try:
     from prettytable import PrettyTable, bcolors
except ModuleNotFoundError as e:
    print(f"Failed to import utilities: {e}")
    sys.exit(1)

##############################
# MAIN (TEST FUNCTION)       #
##############################


def main():
    print("Generated using setters:")
    x = PrettyTable(["City name", "Area", "Population", "Annual Rainfall"])
    x.title = "Australian capital cities"
    x.sortby = "Population"
    x.reversesort = True
    x.int_format["Area"] = "04"
    x.float_format = "6.1"
    x.align["City name"] = "l"  # Left align city names
    x.add_row(["Adelaide", 1295, 1158259, 600.5])
    x.add_row(["Brisbane", 5905, 1857594, 1146.4])
    x.add_row(["Darwin", 112, 120900, 1714.7])
    x.add_row(["Hobart", 1357, 205556, 619.5])
    x.add_row(["Sydney", 2058, 4336374, 1214.8])
    x.add_row(["Melbourne", 1566, 3806092, 646.9])
    x.add_row(["Perth", 5386, 1554769, 869.4])
    print(x)

    print

    print("Generated using constructor arguments:")

    y = PrettyTable(
        ["City name", "Area", "Population", "Annual Rainfall"],
        title="Australian capital cities",
        sortby="Population",
        reversesort=True,
        int_format="04",
        float_format="6.1",
        max_width=12,
        min_width=4,
        align="c",
        valign="t",
    )
    y.align["City name"] = "l"  # Left align city names
    y.add_row(["Adelaide", 1295, 1158259, 600.5])
    y.add_row(["Brisbane", 5905, 1857594, 1146.4])
    y.add_row(["Darwin", 112, 120900, 1714.7])
    y.add_row(["Hobart", 1357, 205556, 619.5])
    y.add_row(["Sydney", 2058, 4336374, 1214.8])
    y.add_row(["Melbourne", 1566, 3806092, 646.9])
    y.add_row(["Perth", 5386, 1554769, 869.4])
    print(y)


if __name__ == "__main__":
    main()