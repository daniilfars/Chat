using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;

namespace ChatSignalR;

public class ChatHub : Hub
{
    private static readonly ConcurrentDictionary<string, string> Users = new();

    public async Task Register(string userName)
    {
        if (string.IsNullOrWhiteSpace(userName))
        {
            await Clients.Caller.SendAsync("RegistrationFailed", "Имя не может быть пустым");
            return;
        }

        if(Users.ContainsKey(userName))
        {
            await Clients.Caller.SendAsync("RegistrationFailed", $"Никнейм '{userName}' уже занят");
            return;
        }

        var oldUser = Users.FirstOrDefault(x => x.Value == Context.ConnectionId).Key;
        if (oldUser != null)
            Users.TryRemove(oldUser, out _);

        Users[userName] = Context.ConnectionId;

        await Clients.Caller.SendAsync("RegistrationSuccess", userName);

        await SendUserList();
    }

    public async Task Send(string message, string name)
    {
        await Clients.All.SendAsync("Receive", name, message);
    }
    public async Task Personal(string to, string message)
    {
        string? userName = Users.FirstOrDefault(x => x.Value == Context.ConnectionId).Key;

        if (Users.TryGetValue(to, out string? userConnectionId))
        {
            await Clients.Client(userConnectionId).SendAsync("ReceivePrivateMessage", userName, message);
            await Clients.Caller.SendAsync("ReceiveCaller", $"Сообщение успешно отправлено пользователю {to}");
        }
        else
        {
            await Clients.Caller.SendAsync("UserOffline", $"Пользователь {to} не найден");
        }
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        var item = Users.FirstOrDefault(u => u.Value == Context.ConnectionId).Key;
        if(item != null)
        {
            Users.TryRemove(item, out _);
            await SendUserList();
        }

        await base.OnDisconnectedAsync(exception);
    }

    public async Task SendUserList()
    {
        var users = Users.Keys.ToList();
        await Clients.All.SendAsync("UpdateUserList", users);
    }
}