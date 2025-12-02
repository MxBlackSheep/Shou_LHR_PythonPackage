# Champions_FL_Python VENUS Backup
### 20251202
- Add support to use new tip-usage management.
- Software test passed, waiting for water-based test. 
- Delete old dependency on sub-methods imported under library folder. Now systematically uses dependency under sub-method. 
### 20251022
- Added comments to clarify VENUS code.
- Support water-based testing, by inserting random readings into the database after FL readings.
- Software-only and water-based test passed (see ExecutionLog)
### 20250826
- Fixed: plateID isn't supplied to VENUS, causing the fluorescent reading upload to fail.
- Modified PlateID and CytomatPosition handling such that Champions_FL::FlourscentReading could be handled and uploaded to DB.