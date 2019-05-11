"SELECT C.*, D.Suburb, D.st1, D.st2
, D.X, D.Y FROM bluetooth_"+(DT_WSTR,4)(@[User::Year])+(DT_WSTR,3)(@[User::Month])+" C, (SELECT B.ID AID, B.LocationID, B.Suburb, B.st1, B.st2, C.ID, C.X, C.Y
							 FROM Area B, Location C
							WHERE B.LocationID = C.ID) D
WHERE C.areaid = D.AID
  AND C.areaid IN ('"+(DT_WSTR,5)(@[User::area1])+"','"+(DT_WSTR,5)(@[User::area2])+"','"+(DT_WSTR,5)(@[User::area3])+"','"+(DT_WSTR,5)(@[User::area4])+"')"