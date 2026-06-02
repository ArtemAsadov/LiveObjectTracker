# send_packet.ps1
param(
    [string]$HostAddr = "127.0.0.1",
    [int]$Port = 5000
)

Write-Host "Connecting to $HostAddr : $Port ..." -ForegroundColor Cyan

try {
    $client = New-Object System.Net.Sockets.TcpClient($HostAddr, $Port)
    $stream = $client.GetStream()

    $json = '{"ObjectId":42,"X":10.5,"Y":20.3,"Z":5.7,"Timestamp":1717300000000}'
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)

    $length = [System.BitConverter]::GetBytes($bytes.Length)

    $stream.Write($length, 0, 4)
    $stream.Write($bytes, 0, $bytes.Length)

    Write-Host "Packet sent! JSON: $json" -ForegroundColor Green
    Write-Host "Payload size: $($bytes.Length) bytes" -ForegroundColor Green

    Start-Sleep -Milliseconds 500
}
catch {
    Write-Host "Error: $_" -ForegroundColor Red
}
finally {
    if ($client) { $client.Close() }
}
