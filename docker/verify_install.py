import sys
import os

print("\nVerification Results:")
print("-" * 40)

home = os.path.expanduser('~')
if home not in sys.executable:
    print("✓ Python location correct (not in home)")
else:
    print("⚠ Python appears to be in home directory")

critical = ['numpy', 'pandas', 'xarray', 'osgeo.gdal', 'rasterio', 'netCDF4', 'torch']
failed = []

for pkg in critical:
    try:
        if '.' in pkg:
            parts = pkg.split('.')
            mod = __import__(pkg)
            for part in parts[1:]:
                mod = getattr(mod, part)
        else:
            mod = __import__(pkg)
        version = getattr(mod, '__version__', 'unknown')
        print(f"✓ {pkg} imported successfully (version: {version})")
    except ImportError as e:
        print(f"✗ {pkg} import failed: {e}")
        failed.append(pkg)

try:
    from geocif import geocif_runner
    print("✓ geocif imported successfully")
except ImportError as e:
    print(f"✗ geocif import failed: {e}")
    failed.append('geocif')

try:
    import geoprepare
    print(f"✓ geoprepare imported successfully (version: {getattr(geoprepare, '__version__', 'unknown')})")
except ImportError as e:
    print(f"✗ geoprepare import failed: {e}")
    failed.append('geoprepare')

if not failed:
    print("\n✅ All critical packages verified!")
else:
    print(f"\n⚠ Failed packages: {', '.join(failed)}")
    print("You can try installing them manually with:")
    print(f"  uv pip install {' '.join(failed)}")
    sys.exit(1)
