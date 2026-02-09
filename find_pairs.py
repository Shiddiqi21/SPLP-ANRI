from app.database import get_db_context
from sqlalchemy import text

def find_pairs():
    with get_db_context() as db:
        units = db.execute(text("SELECT id, nama FROM unit_kerja")).mappings().all()
        
        # Build map {clean_name: [list of full_names/ids]}
        # We try to detect if "X" and "X (Lama)" exist.
        
        clean_map = {}
        
        for u in units:
            name = u['nama'].strip()
            # Remove "(Lama)" from end if exists to get 'base' name
            base = name.replace(" (Lama)", "").replace("(Lama)", "").strip()
            
            if base not in clean_map:
                clean_map[base] = []
            clean_map[base].append(u)
            
        # Filter for entries with > 1 unit sharing the base name
        print(f"Total Unique Bases: {len(clean_map)}")
        
        duplicates_found = 0
        to_delete = []
        
        print("\n--- Potential Duplicate Pairs ---")
        for base, group in clean_map.items():
            if len(group) > 1:
                print(f"Base '{base}':")
                duplicates_found += 1
                
                # Identify which one is "Lama" (legacy) to candidate for deletion
                # Heuristic: Name contains "(Lama)" or ID is higher/lower?
                # User said "hapus yg duplikat", usually implies the older/legacy one, or the one explicitly marked "(Lama)".
                
                for u in group:
                    print(f"  - [{u['id']}] {u['nama']}")
                    if "(Lama)" in u['nama']:
                        to_delete.append(u['id'])
        
        print(f"\nFound {duplicates_found} groups with duplicates.")
        print(f"Candidates for deletion (contain '(Lama)'): {len(to_delete)}")
        print(f"IDs: {to_delete}")
        
        current_count = len(units)
        target_after = current_count - len(to_delete)
        print(f"Current: {current_count}, Target: 77. Projected: {target_after}")

if __name__ == "__main__":
    find_pairs()
